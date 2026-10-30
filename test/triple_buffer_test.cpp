#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <thread>

#include "cyberverse/algorithm/container/triple_buffer.hpp"

using cyberverse::algorithm::triple_buffer;

TEST_CASE("triple_buffer publishes and reads the latest value", "[algorithm][triple_buffer]") {
  triple_buffer<int> buffer;
  CHECK(buffer.read() == 0);

  buffer.write_slot() = 1;
  buffer.publish();
  CHECK(buffer.read() == 1);
  CHECK(buffer.read() == 1);

  buffer.write_slot() = 2;
  buffer.publish();
  buffer.write_slot() = 3;
  buffer.publish();
  CHECK(buffer.read() == 3);
}

TEST_CASE("triple_buffer keeps a reader view stable while publishing", "[algorithm][triple_buffer]") {
  triple_buffer<int> buffer;
  buffer.write_slot() = 10;
  buffer.publish();

  const int& view = buffer.read();
  CHECK(view == 10);

  buffer.write_slot() = 20;
  buffer.publish();
  buffer.write_slot() = 30;
  buffer.publish();

  CHECK(view == 10);
  CHECK(buffer.read() == 30);
}

TEST_CASE("triple_buffer supports concurrent writer and reader", "[algorithm][triple_buffer]") {
  triple_buffer<int> buffer;
  constexpr int count = 10000;
  std::atomic<bool> done{ false };

  std::thread writer([&] {
    for (int value = 1; value <= count; ++value) {
      buffer.write_slot() = value;
      buffer.publish();
    }
    done.store(true, std::memory_order_release);
  });

  int last = 0;
  while (!done.load(std::memory_order_acquire)) {
    const int value = buffer.read();
    CHECK(value >= last);
    last = value;
    std::this_thread::yield();
  }
  writer.join();

  CHECK(buffer.read() == count);
}
