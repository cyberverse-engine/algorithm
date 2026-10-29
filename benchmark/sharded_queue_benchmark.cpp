#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "cyberverse/algorithm/container/sharded_queue.hpp"

using cyberverse::algorithm::sharded_queue;

namespace {

constexpr std::size_t chunk_size = 64;
using queue_type = sharded_queue<std::uint64_t, chunk_size>;

std::size_t pool_capacity_for(std::size_t item_count) { return item_count / chunk_size + 2; }

} // namespace

/**
 * Push a batch through one producer, flush it, and drain it again. This is the
 * steady-state round trip of the queue and includes the consumer-side move of
 * every item.
 */
static void BM_SingleProducerPushDrain(benchmark::State& state) {
  const auto batch = static_cast<std::size_t>(state.range(0));

  auto queue = std::make_shared<queue_type>(pool_capacity_for(batch), 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  if (producer == nullptr) {
    state.SkipWithError("failed to acquire producer");
    return;
  }

  std::vector<std::uint64_t> items;
  items.reserve(batch);

  for (auto _ : state) {
    for (std::size_t i = 0; i < batch; ++i) {
      producer->push(static_cast<std::uint64_t>(i));
    }
    producer->flush();

    items.clear();
    queue->drain_into(items);
    benchmark::DoNotOptimize(items.data());
    benchmark::DoNotOptimize(items.size());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch * sizeof(std::uint64_t)));
  state.counters["dropped"] = static_cast<double>(queue->dropped_count());
}

/**
 * Measure only the write path: each timed iteration pushes and flushes a batch,
 * while draining the previous batch happens with the timer paused.
 */
static void BM_SingleProducerPushOnly(benchmark::State& state) {
  const auto batch = static_cast<std::size_t>(state.range(0));

  auto queue = std::make_shared<queue_type>(pool_capacity_for(batch), 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  if (producer == nullptr) {
    state.SkipWithError("failed to acquire producer");
    return;
  }

  for (auto _ : state) {
    state.PauseTiming();
    queue->drain();
    state.ResumeTiming();

    for (std::size_t i = 0; i < batch; ++i) {
      producer->push(static_cast<std::uint64_t>(i));
    }
    producer->flush();
  }

  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch * sizeof(std::uint64_t)));
  state.counters["dropped"] = static_cast<double>(queue->dropped_count());
}

/**
 * Spawn a fixed number of writer threads per iteration and drain the queue on
 * the benchmark thread. Includes thread creation/join and the final drain, so
 * it measures end-to-end batch throughput rather than a single push.
 */
template <std::size_t ProducerCount>
static void BM_ConcurrentProducers(benchmark::State& state) {
  const auto items_per_producer = static_cast<std::size_t>(state.range(0));
  const auto total = ProducerCount * items_per_producer;

  auto queue = std::make_shared<queue_type>(ProducerCount * pool_capacity_for(items_per_producer), ProducerCount);
  std::vector<std::uint64_t> items;
  items.reserve(total);

  for (auto _ : state) {
    std::vector<std::thread> producers;
    producers.reserve(ProducerCount);
    for (std::size_t id = 0; id < ProducerCount; ++id) {
      producers.emplace_back([queue, id, items_per_producer] {
        const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
        if (producer == nullptr) {
          return;
        }
        const std::uint64_t base = static_cast<std::uint64_t>(id) * items_per_producer;
        for (std::size_t i = 0; i < items_per_producer; ++i) {
          producer->push(base + static_cast<std::uint64_t>(i));
        }
        producer->flush();
      });
    }
    for (std::thread& producer : producers) {
      producer.join();
    }

    items.clear();
    queue->drain_into(items);
    benchmark::DoNotOptimize(items.data());
    benchmark::DoNotOptimize(items.size());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(total * sizeof(std::uint64_t)));
  state.counters["dropped"] = static_cast<double>(queue->dropped_count());
}

/**
 * Measure the cost of polling a producer that has no pending flush request.
 */
static void BM_FlushRequestPoll(benchmark::State& state) {
  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  if (producer == nullptr) {
    state.SkipWithError("failed to acquire producer");
    return;
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(producer->flush_if_requested());
  }

  state.SetItemsProcessed(state.iterations());
}

/**
 * Push a batch, request a flush-all, handle it on the producer, and drain.
 * This is the request-driven equivalent of the push/drain round trip.
 */
static void BM_RequestFlushRoundTrip(benchmark::State& state) {
  const auto batch = static_cast<std::size_t>(state.range(0));

  auto queue = std::make_shared<queue_type>(pool_capacity_for(batch), 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  if (producer == nullptr) {
    state.SkipWithError("failed to acquire producer");
    return;
  }

  std::vector<std::uint64_t> items;
  items.reserve(batch);

  for (auto _ : state) {
    for (std::size_t i = 0; i < batch; ++i) {
      producer->push(static_cast<std::uint64_t>(i));
    }
    queue->request_flush_all();
    benchmark::DoNotOptimize(producer->flush_if_requested());

    items.clear();
    queue->drain_into(items);
    benchmark::DoNotOptimize(items.data());
    benchmark::DoNotOptimize(items.size());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch * sizeof(std::uint64_t)));
  state.counters["dropped"] = static_cast<double>(queue->dropped_count());
}

BENCHMARK(BM_SingleProducerPushDrain)->Arg(1024)->Arg(16384)->Arg(262144);
BENCHMARK(BM_SingleProducerPushOnly)->Arg(1024)->Arg(16384)->Arg(262144);
BENCHMARK(BM_FlushRequestPoll);
BENCHMARK(BM_RequestFlushRoundTrip)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ConcurrentProducers, 1)->Arg(65536)->UseRealTime();
BENCHMARK_TEMPLATE(BM_ConcurrentProducers, 2)->Arg(65536)->UseRealTime();
BENCHMARK_TEMPLATE(BM_ConcurrentProducers, 4)->Arg(65536)->UseRealTime();
BENCHMARK_TEMPLATE(BM_ConcurrentProducers, 8)->Arg(65536)->UseRealTime();

BENCHMARK_MAIN();
