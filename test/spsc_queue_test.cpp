#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <thread>
#include <vector>

#include "cyberverse/algorithm/container/spsc_queue.hpp"

using cyberverse::algorithm::spsc_queue;

TEST_CASE("spsc_queue validates capacity", "[algorithm][spsc_queue]") {
  CHECK_THROWS_AS(spsc_queue<int>(0), std::invalid_argument);
  CHECK_THROWS_AS(spsc_queue<int>(1), std::invalid_argument);

  spsc_queue<int> queue(4);
  CHECK(queue.capacity() == 4);
  CHECK(queue.usable_capacity() == 3);
  CHECK(queue.empty());
  CHECK(queue.size() == 0);
}

TEST_CASE("spsc_queue preserves FIFO order and wraps", "[algorithm][spsc_queue]") {
  spsc_queue<int> queue(4);
  CHECK(queue.try_push(1));
  CHECK(queue.try_push(2));
  CHECK(queue.try_push(3));
  CHECK_FALSE(queue.try_push(4));

  int value = 0;
  CHECK(queue.try_pop(value));
  CHECK(value == 1);
  CHECK(queue.try_push(4));

  std::vector<int> values;
  while (queue.try_pop(value)) {
    values.push_back(value);
  }
  REQUIRE(values.size() == 3);
  CHECK(values[0] == 2);
  CHECK(values[1] == 3);
  CHECK(values[2] == 4);
  CHECK(queue.empty());
}

TEST_CASE("spsc_queue supports one producer and one consumer", "[algorithm][spsc_queue]") {
  spsc_queue<int> queue(16);
  constexpr int count = 10000;
  std::atomic<bool> producer_done{ false };

  std::thread producer([&] {
    for (int value = 0; value < count; ++value) {
      while (!queue.try_push(value)) {
        std::this_thread::yield();
      }
    }
    producer_done.store(true, std::memory_order_release);
  });

  std::vector<int> received;
  received.reserve(count);
  std::thread consumer([&] {
    int value = 0;
    while (!producer_done.load(std::memory_order_acquire) || !queue.empty()) {
      if (queue.try_pop(value)) {
        received.push_back(value);
      } else {
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  REQUIRE(received.size() == count);
  for (int index = 0; index < count; ++index) {
    CHECK(received[static_cast<std::size_t>(index)] == index);
  }
}
