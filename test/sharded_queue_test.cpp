#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "cyberverse/algorithm.hpp"

using cyberverse::algorithm::multithread_sharded_queue;
using cyberverse::algorithm::sharded_queue;

namespace {

struct move_only_item {
  explicit move_only_item(int value) : value(value) {}

  move_only_item(const move_only_item&) = delete;
  move_only_item& operator=(const move_only_item&) = delete;
  move_only_item(move_only_item&& other) noexcept : value(other.value) { other.value = -1; }
  move_only_item& operator=(move_only_item&& other) noexcept {
    value = other.value;
    other.value = -1;
    return *this;
  }

  int value = 0;
};

struct counted_item {
  static std::atomic<int>& alive() {
    static std::atomic<int> count{ 0 };
    return count;
  }

  explicit counted_item(int value) : value(value) { alive().fetch_add(1, std::memory_order_relaxed); }
  counted_item(const counted_item& other) : value(other.value) { alive().fetch_add(1, std::memory_order_relaxed); }
  counted_item(counted_item&& other) noexcept : value(other.value) { alive().fetch_add(1, std::memory_order_relaxed); }
  counted_item& operator=(const counted_item&) = delete;
  counted_item& operator=(counted_item&&) = delete;
  ~counted_item() { alive().fetch_sub(1, std::memory_order_relaxed); }

  int value = 0;
};

template <typename Item>
bool contains_exact_sequence(const std::vector<Item>& items, std::size_t expected_size) {
  if (items.size() != expected_size) {
    return false;
  }
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (items[i] != static_cast<Item>(i)) {
      return false;
    }
  }
  return true;
}

} // namespace

TEST_CASE("sharded_queue drains only published chunks in writer order", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(8, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  for (int i = 0; i < 10; ++i) {
    producer->push(i);
  }

  // Eight items filled two chunks; the last two are still in the open chunk.
  CHECK(queue->pending_chunks() == 2);
  CHECK(queue->drain() == std::vector<int>{ 0, 1, 2, 3, 4, 5, 6, 7 });
  CHECK(queue->pending_chunks() == 0);
  CHECK(queue->drain().empty());

  producer->flush();
  CHECK(queue->pending_chunks() == 1);
  CHECK(queue->drain() == std::vector<int>{ 8, 9 });
  CHECK(queue->pending_chunks() == 0);
  CHECK(queue->dropped_count() == 0);
}

TEST_CASE("sharded_queue drain_into appends and can reuse storage", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 2>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  producer->push(2);
  producer->push(3);
  producer->flush();

  std::vector<int> out{ -1, 0 };
  queue->drain_into(out);
  CHECK(out == std::vector<int>{ -1, 0, 1, 2, 3 });

  queue->drain_into(out);
  CHECK(out.size() == 5);
}

TEST_CASE("sharded_queue flushes buffered items when a producer is destroyed", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 8>;

  auto queue = std::make_shared<queue_type>(2, 1);
  {
    const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
    REQUIRE(producer != nullptr);
    producer->push(1);
    producer->push(2);
    CHECK(queue->pending_chunks() == 0);
  }

  CHECK(queue->pending_chunks() == 1);
  CHECK(queue->drain() == std::vector<int>{ 1, 2 });
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue drops only when the chunk pool is exhausted", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 2>;

  auto queue = std::make_shared<queue_type>(1, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  producer->push(2);
  producer->push(3);
  CHECK(queue->dropped_count() == 1);
  CHECK(queue->pending_chunks() == 1);

  CHECK(queue->drain() == std::vector<int>{ 1, 2 });

  producer->push(3);
  producer->flush();
  CHECK(queue->drain() == std::vector<int>{ 3 });
  CHECK(queue->dropped_count() == 1);
}

TEST_CASE("sharded_queue recycles chunks across many drain rounds", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(2, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  int next = 0;
  for (int round = 0; round < 1000; ++round) {
    for (int i = 0; i < 8; ++i) {
      producer->push(next++);
    }
    producer->flush();

    std::vector<int> items = queue->drain();
    REQUIRE(items.size() == 8);
    for (int i = 0; i < 8; ++i) {
      CHECK(items[static_cast<std::size_t>(i)] == round * 8 + i);
    }
  }

  CHECK(queue->dropped_count() == 0);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue keeps per-writer order with concurrent writers", "[algorithm][sharded_queue]") {
  constexpr int producer_count = 4;
  constexpr int items_per_producer = 2000;
  using queue_type = sharded_queue<int, 64>;

  auto queue = std::make_shared<queue_type>(256, producer_count);
  std::atomic<bool> acquisition_failed{ false };
  std::vector<std::thread> producers;
  producers.reserve(producer_count);

  for (int id = 0; id < producer_count; ++id) {
    producers.emplace_back([queue, &acquisition_failed, id] {
      const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
      if (producer == nullptr) {
        acquisition_failed.store(true, std::memory_order_release);
        return;
      }
      for (int i = 0; i < items_per_producer; ++i) {
        producer->push(id * items_per_producer + i);
      }
      producer->flush();
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }

  CHECK_FALSE(acquisition_failed.load(std::memory_order_acquire));

  const std::vector<int> items = queue->drain();
  REQUIRE(items.size() == static_cast<std::size_t>(producer_count * items_per_producer));

  std::vector<int> next_index(producer_count, 0);
  bool order_ok = true;
  for (const int item : items) {
    const int id = item / items_per_producer;
    const int index = item % items_per_producer;
    if (id < 0 || id >= producer_count || index != next_index[static_cast<std::size_t>(id)]) {
      order_ok = false;
      break;
    }
    ++next_index[static_cast<std::size_t>(id)];
  }

  CHECK(order_ok);
  for (const int count : next_index) {
    CHECK(count == items_per_producer);
  }
  CHECK(queue->dropped_count() == 0);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue drains while producers are still writing", "[algorithm][sharded_queue]") {
  constexpr int producer_count = 4;
  constexpr int items_per_producer = 20000;
  using queue_type = sharded_queue<std::uint64_t, 128>;

  auto queue = std::make_shared<queue_type>(1024, producer_count);
  std::atomic<bool> producers_done{ false };
  std::atomic<bool> acquisition_failed{ false };
  std::vector<std::uint64_t> received;

  std::thread consumer([&] {
    while (!producers_done.load(std::memory_order_acquire)) {
      queue->drain_into(received);
      std::this_thread::yield();
    }
    queue->drain_into(received);
  });

  std::vector<std::thread> producers;
  producers.reserve(producer_count);
  for (int id = 0; id < producer_count; ++id) {
    producers.emplace_back([queue, &acquisition_failed, id] {
      const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
      if (producer == nullptr) {
        acquisition_failed.store(true, std::memory_order_release);
        return;
      }
      const std::uint64_t base = static_cast<std::uint64_t>(id) * items_per_producer;
      for (int i = 0; i < items_per_producer; ++i) {
        producer->push(base + static_cast<std::uint64_t>(i));
      }
      producer->flush();
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }
  producers_done.store(true, std::memory_order_release);
  consumer.join();

  CHECK_FALSE(acquisition_failed.load(std::memory_order_acquire));

  const auto total = static_cast<std::size_t>(producer_count) * items_per_producer;
  REQUIRE(received.size() == total);

  std::sort(received.begin(), received.end());
  bool sequence_ok = true;
  for (std::size_t i = 0; i < received.size(); ++i) {
    if (received[i] != static_cast<std::uint64_t>(i)) {
      sequence_ok = false;
      break;
    }
  }
  CHECK(sequence_ok);
  CHECK(queue->dropped_count() == 0);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue stores move-only items", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<move_only_item, 2>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->emplace(7);
  producer->push(move_only_item(8));
  producer->flush();

  const std::vector<move_only_item> items = queue->drain();
  REQUIRE(items.size() == 2);
  CHECK(items[0].value == 7);
  CHECK(items[1].value == 8);
}

TEST_CASE("sharded_queue destroys undrained and drained items", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<counted_item, 4>;

  REQUIRE(counted_item::alive().load(std::memory_order_relaxed) == 0);
  {
    auto queue = std::make_shared<queue_type>(4, 1);
    {
      const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
      REQUIRE(producer != nullptr);
      producer->emplace(1);
      producer->emplace(2);
      producer->flush();
      CHECK(counted_item::alive().load(std::memory_order_relaxed) == 2);

      {
        const std::vector<counted_item> items = queue->drain();
        REQUIRE(items.size() == 2);
        CHECK(counted_item::alive().load(std::memory_order_relaxed) == 2);
      }
      CHECK(counted_item::alive().load(std::memory_order_relaxed) == 0);

      producer->emplace(3);
      producer->emplace(4);
      CHECK(counted_item::alive().load(std::memory_order_relaxed) == 2);
    }
    // The producer flushed the open chunk on destruction; the queue still owns
    // its items until it is destroyed.
    CHECK(counted_item::alive().load(std::memory_order_relaxed) == 2);
  }
  CHECK(counted_item::alive().load(std::memory_order_relaxed) == 0);
}

TEST_CASE("sharded_queue limits the number of registered producers", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(8, 2);
  std::shared_ptr<queue_type::producer> first = queue->acquire_producer();
  std::shared_ptr<queue_type::producer> second = queue->acquire_producer();
  REQUIRE(first != nullptr);
  REQUIRE(second != nullptr);
  CHECK(queue->acquire_producer() == nullptr);

  first.reset();
  const std::shared_ptr<queue_type::producer> replacement = queue->acquire_producer();
  CHECK(replacement != nullptr);
  CHECK(queue->acquire_producer() == nullptr);
}

TEST_CASE("sharded_queue flush publishes at most once", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->flush();
  CHECK(queue->pending_chunks() == 0);

  producer->push(1);
  CHECK(queue->pending_chunks() == 0);
  producer->flush();
  CHECK(queue->pending_chunks() == 1);
  producer->flush();
  CHECK(queue->pending_chunks() == 1);
}

TEST_CASE("sharded_queue take_completed transfers chunks to the caller", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 2>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  producer->push(2);
  producer->flush();
  CHECK(queue->pending_chunks() == 1);

  queue_type::chunk* const taken = producer->take_completed();
  REQUIRE(taken != nullptr);
  CHECK(taken->size() == 2);
  CHECK(taken->header.next == nullptr);
  CHECK(producer->take_completed() == nullptr);
  CHECK(queue->pending_chunks() == 1);

  producer->reuse_chunk(taken);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("multithread_sharded_queue creates one producer per calling thread", "[algorithm][sharded_queue]") {
  constexpr int producer_count = 4;
  constexpr int items_per_producer = 1000;
  using queue_type = sharded_queue<int, 16>;

  auto queue = std::make_shared<queue_type>(512, producer_count);
  multithread_sharded_queue<int, 16> shared_queue(queue);

  std::vector<std::thread> producers;
  producers.reserve(producer_count);
  for (int id = 0; id < producer_count; ++id) {
    producers.emplace_back([&shared_queue, id] {
      for (int i = 0; i < items_per_producer; ++i) {
        shared_queue.push(id * items_per_producer + i);
      }
      shared_queue.flush();
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }

  const std::vector<int> items = shared_queue.drain();
  REQUIRE(items.size() == static_cast<std::size_t>(producer_count * items_per_producer));

  std::vector<int> next_index(producer_count, 0);
  bool order_ok = true;
  for (const int item : items) {
    const int id = item / items_per_producer;
    const int index = item % items_per_producer;
    if (id < 0 || id >= producer_count || index != next_index[static_cast<std::size_t>(id)]) {
      order_ok = false;
      break;
    }
    ++next_index[static_cast<std::size_t>(id)];
  }

  CHECK(order_ok);
  for (const int count : next_index) {
    CHECK(count == items_per_producer);
  }
  CHECK(shared_queue.dropped_count() == 0);
  CHECK(shared_queue.pending_chunks() == 0);
}

TEST_CASE("multithread_sharded_queue supports the main thread", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(32, 4);
  multithread_sharded_queue<int, 4> shared_queue(queue);

  shared_queue.push(1);
  shared_queue.emplace(2);
  shared_queue.flush();

  CHECK(shared_queue.drain() == std::vector<int>{ 1, 2 });
  CHECK(shared_queue.dropped_count() == 0);
}

TEST_CASE("multithread_sharded_queue reports drops when producer slots are full", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(8, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  multithread_sharded_queue<int, 4> shared_queue(queue);
  shared_queue.push(1);
  CHECK(shared_queue.dropped_count() == 1);

  producer->push(2);
  producer->flush();
  CHECK(shared_queue.drain() == std::vector<int>{ 2 });
}

TEST_CASE("multithread_sharded_queue rejects a null queue", "[algorithm][sharded_queue]") {
  CHECK_THROWS_AS((multithread_sharded_queue<int, 4>(nullptr)), std::invalid_argument);
}

TEST_CASE("sharded_queue supports a chunk size of one", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 1>;

  auto queue = std::make_shared<queue_type>(8, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  for (int i = 0; i < 8; ++i) {
    producer->push(i);
  }
  producer->flush();

  CHECK(queue->pending_chunks() == 8);
  CHECK(contains_exact_sequence(queue->drain(), 8));
  CHECK(queue->dropped_count() == 0);
}

TEST_CASE("sharded_queue drains leftover chunks when a writer slot is reused", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(4, 1);
  {
    const std::shared_ptr<queue_type::producer> first = queue->acquire_producer();
    REQUIRE(first != nullptr);
    first->push(1);
    first->push(2);
    first->flush();
  }

  const std::shared_ptr<queue_type::producer> second = queue->acquire_producer();
  REQUIRE(second != nullptr);
  second->push(3);
  second->flush();

  CHECK(queue->drain() == std::vector<int>{ 1, 2, 3 });
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue recycles a non-power-of-two chunk pool", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(3, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  int next = 0;
  for (int round = 0; round < 1000; ++round) {
    for (int i = 0; i < 12; ++i) {
      producer->push(next++);
    }
    producer->flush();

    const std::vector<int> items = queue->drain();
    REQUIRE(items.size() == 12);
    for (int i = 0; i < 12; ++i) {
      CHECK(items[static_cast<std::size_t>(i)] == round * 12 + i);
    }
  }

  CHECK(queue->dropped_count() == 0);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue accounts for every item under a small pooled writer race", "[algorithm][sharded_queue]") {
  constexpr int producer_count = 2;
  constexpr int items_per_producer = 5000;
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(3, producer_count);
  std::atomic<bool> producers_done{ false };
  std::atomic<bool> acquisition_failed{ false };
  std::vector<int> received;

  std::thread consumer([&] {
    while (!producers_done.load(std::memory_order_acquire)) {
      queue->drain_into(received);
      std::this_thread::yield();
    }
    queue->drain_into(received);
  });

  std::vector<std::thread> producers;
  producers.reserve(producer_count);
  for (int id = 0; id < producer_count; ++id) {
    producers.emplace_back([queue, &acquisition_failed, id] {
      const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
      if (producer == nullptr) {
        acquisition_failed.store(true, std::memory_order_release);
        return;
      }
      for (int i = 0; i < items_per_producer; ++i) {
        producer->push(id * items_per_producer + i);
      }
      producer->flush();
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }
  producers_done.store(true, std::memory_order_release);
  consumer.join();

  CHECK_FALSE(acquisition_failed.load(std::memory_order_acquire));

  const auto total = static_cast<std::uint64_t>(producer_count) * items_per_producer;
  CHECK(static_cast<std::uint64_t>(received.size()) + queue->dropped_count() == total);

  std::vector<int> last(producer_count, -1);
  bool order_ok = true;
  for (const int item : received) {
    const int id = item / items_per_producer;
    if (id < 0 || id >= producer_count || item <= last[static_cast<std::size_t>(id)]) {
      order_ok = false;
      break;
    }
    last[static_cast<std::size_t>(id)] = item;
  }
  CHECK(order_ok);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue survives writer churn while draining", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 8>;

  auto queue = std::make_shared<queue_type>(8, 2);
  std::atomic<bool> producers_done{ false };
  std::vector<int> received;

  std::thread consumer([&] {
    while (!producers_done.load(std::memory_order_acquire)) {
      queue->drain_into(received);
      std::this_thread::yield();
    }
    queue->drain_into(received);
  });

  int sent = 0;
  for (int round = 0; round < 2000; ++round) {
    const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
    REQUIRE(producer != nullptr);
    for (int i = 0; i < 8; ++i) {
      producer->push(sent++);
    }
    if (round % 3 == 0) {
      producer->flush();
    }
    // The producer is destroyed at the end of the iteration and flushes any
    // remaining items while the consumer may be scanning the writer slot.
  }
  producers_done.store(true, std::memory_order_release);
  consumer.join();

  CHECK(static_cast<std::uint64_t>(received.size()) + queue->dropped_count() == static_cast<std::uint64_t>(sent));
  bool order_ok = true;
  for (std::size_t i = 1; i < received.size(); ++i) {
    if (received[i] <= received[i - 1]) {
      order_ok = false;
      break;
    }
  }
  CHECK(order_ok);
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue request_flush_all is cooperative", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 8>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  producer->push(2);
  CHECK(queue->pending_chunks() == 0);

  queue->request_flush_all();

  // The request alone does not publish anything; a producer must observe it.
  CHECK(queue->drain().empty());
  CHECK(queue->pending_chunks() == 0);

  CHECK(producer->flush_if_requested());
  CHECK(queue->pending_chunks() == 1);
  CHECK(queue->drain() == std::vector<int>{ 1, 2 });
  CHECK_FALSE(producer->flush_if_requested());
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue coalesces flush requests per producer", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  queue->request_flush_all();
  queue->request_flush_all();

  CHECK(producer->flush_if_requested());
  CHECK_FALSE(producer->flush_if_requested());
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue push and emplace observe flush requests before writing", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 8>;

  auto queue = std::make_shared<queue_type>(8, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  producer->push(2);
  queue->request_flush_all();

  producer->push(3);
  CHECK(queue->drain() == std::vector<int>{ 1, 2 });
  CHECK_FALSE(producer->flush_if_requested());
  producer->flush();
  CHECK(queue->drain() == std::vector<int>{ 3 });

  producer->emplace(4);
  queue->request_flush_all();
  producer->emplace(5);
  CHECK(queue->drain() == std::vector<int>{ 4 });
  producer->flush();
  CHECK(queue->drain() == std::vector<int>{ 5 });
}

TEST_CASE("sharded_queue explicit flush acknowledges visible requests", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(4, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  queue->request_flush_all();
  producer->flush();

  CHECK_FALSE(producer->flush_if_requested());
  CHECK(queue->drain() == std::vector<int>{ 1 });

  queue->request_flush_all();
  producer->flush();
  CHECK_FALSE(producer->flush_if_requested());
}

TEST_CASE("sharded_queue request_flush_all does not affect producers acquired later", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(4, 1);
  queue->request_flush_all();

  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);
  producer->push(1);

  CHECK_FALSE(producer->flush_if_requested());
  producer->flush();
  CHECK(queue->drain() == std::vector<int>{ 1 });
}

TEST_CASE("sharded_queue flushes every producer on request", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(8, 2);
  const std::shared_ptr<queue_type::producer> first = queue->acquire_producer();
  const std::shared_ptr<queue_type::producer> second = queue->acquire_producer();
  REQUIRE(first != nullptr);
  REQUIRE(second != nullptr);

  first->push(0);
  first->push(1);
  second->push(100);
  second->push(101);

  queue->request_flush_all();
  CHECK(first->flush_if_requested());
  CHECK(second->flush_if_requested());
  CHECK_FALSE(first->flush_if_requested());
  CHECK_FALSE(second->flush_if_requested());

  const std::vector<int> items = queue->drain();
  std::vector<int> first_items;
  std::vector<int> second_items;
  for (const int item : items) {
    if (item < 100) {
      first_items.push_back(item);
    } else {
      second_items.push_back(item);
    }
  }
  CHECK(first_items == std::vector<int>{ 0, 1 });
  CHECK(second_items == std::vector<int>{ 100, 101 });
  CHECK(queue->pending_chunks() == 0);
}

TEST_CASE("sharded_queue request flush can precede a drop when the pool is exhausted", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 2>;

  auto queue = std::make_shared<queue_type>(1, 1);
  const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
  REQUIRE(producer != nullptr);

  producer->push(1);
  producer->push(2);
  queue->request_flush_all();
  producer->push(3);

  CHECK(queue->dropped_count() == 1);
  CHECK(queue->drain() == std::vector<int>{ 1, 2 });
  CHECK_FALSE(producer->flush_if_requested());
}

TEST_CASE("multithread_sharded_queue forwards flush requests", "[algorithm][sharded_queue]") {
  using queue_type = sharded_queue<int, 4>;

  auto queue = std::make_shared<queue_type>(8, 2);
  multithread_sharded_queue<int, 4> shared_queue(queue);

  shared_queue.push(1);
  shared_queue.push(2);
  shared_queue.request_flush_all();

  CHECK(shared_queue.flush_if_requested());
  CHECK(shared_queue.drain() == std::vector<int>{ 1, 2 });
  CHECK_FALSE(shared_queue.flush_if_requested());

  auto empty_queue = std::make_shared<queue_type>(8, 1);
  multithread_sharded_queue<int, 4> empty_shared_queue(empty_queue);
  empty_shared_queue.request_flush_all();
  CHECK_FALSE(empty_shared_queue.flush_if_requested());

  empty_shared_queue.push(3);
  CHECK_FALSE(empty_shared_queue.flush_if_requested());
  empty_shared_queue.flush();
  CHECK(empty_shared_queue.drain() == std::vector<int>{ 3 });
}

TEST_CASE("sharded_queue handles flush requests while producers are writing", "[algorithm][sharded_queue]") {
  constexpr int producer_count = 4;
  constexpr int items_per_producer = 5000;
  using queue_type = sharded_queue<int, 8>;

  auto queue = std::make_shared<queue_type>(8192, producer_count);
  std::atomic<bool> producers_done{ false };
  std::atomic<bool> acquisition_failed{ false };
  std::vector<int> received;

  std::thread consumer([&] {
    while (!producers_done.load(std::memory_order_acquire)) {
      queue->drain_into(received);
      std::this_thread::yield();
    }
    queue->drain_into(received);
  });

  std::vector<std::thread> producers;
  producers.reserve(producer_count);
  for (int id = 0; id < producer_count; ++id) {
    producers.emplace_back([queue, &acquisition_failed, id] {
      const std::shared_ptr<queue_type::producer> producer = queue->acquire_producer();
      if (producer == nullptr) {
        acquisition_failed.store(true, std::memory_order_release);
        return;
      }
      for (int i = 0; i < items_per_producer; ++i) {
        producer->push(id * items_per_producer + i);
      }
      producer->flush();
    });
  }

  std::thread requester([queue] {
    for (int i = 0; i < 1000; ++i) {
      queue->request_flush_all();
      std::this_thread::yield();
    }
  });

  for (std::thread& producer : producers) {
    producer.join();
  }
  requester.join();
  producers_done.store(true, std::memory_order_release);
  consumer.join();

  CHECK_FALSE(acquisition_failed.load(std::memory_order_acquire));
  const auto total = static_cast<std::size_t>(producer_count) * items_per_producer;
  REQUIRE(received.size() == total);

  std::vector<int> last(producer_count, -1);
  bool order_ok = true;
  for (const int item : received) {
    const int id = item / items_per_producer;
    if (id < 0 || id >= producer_count || item <= last[static_cast<std::size_t>(id)]) {
      order_ok = false;
      break;
    }
    last[static_cast<std::size_t>(id)] = item;
  }
  CHECK(order_ok);

  std::sort(received.begin(), received.end());
  bool sequence_ok = true;
  for (std::size_t i = 0; i < received.size(); ++i) {
    if (received[i] != static_cast<int>(i)) {
      sequence_ok = false;
      break;
    }
  }
  CHECK(sequence_ok);
  CHECK(queue->dropped_count() == 0);
  CHECK(queue->pending_chunks() == 0);
}
