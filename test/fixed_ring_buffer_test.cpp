#include <catch2/catch_test_macros.hpp>

#include <string>

#include "cyberverse/algorithm/container/fixed_ring_buffer.hpp"

using cyberverse::algorithm::fixed_ring_buffer;

TEST_CASE("fixed_ring_buffer starts empty", "[algorithm][fixed_ring_buffer]") {
  fixed_ring_buffer<int, 4> buffer;

  CHECK(buffer.capacity == 4);
  CHECK(buffer.empty());
  CHECK_FALSE(buffer.full());
  CHECK(buffer.size() == 0);
}

TEST_CASE("fixed_ring_buffer keeps logical order before wrapping", "[algorithm][fixed_ring_buffer]") {
  fixed_ring_buffer<int, 4> buffer;
  buffer.push(10);
  buffer.push(20);
  buffer.push(30);

  REQUIRE(buffer.size() == 3);
  CHECK(buffer.front() == 10);
  CHECK(buffer.back() == 30);
  CHECK(buffer.at(0) == 10);
  CHECK(buffer.at(1) == 20);
  CHECK(buffer.at(2) == 30);
  CHECK(buffer[0] == 10);
  CHECK(buffer[2] == 30);
  CHECK_FALSE(buffer.full());
}

TEST_CASE("fixed_ring_buffer overwrites the oldest element when full", "[algorithm][fixed_ring_buffer]") {
  fixed_ring_buffer<int, 4> buffer;
  for (int value = 0; value < 6; ++value) {
    buffer.push(value);
  }

  CHECK(buffer.full());
  CHECK(buffer.size() == 4);
  CHECK(buffer.front() == 2);
  CHECK(buffer.back() == 5);
  for (std::size_t index = 0; index < buffer.size(); ++index) {
    CHECK(buffer.at(index) == static_cast<int>(index + 2));
  }
}

TEST_CASE("fixed_ring_buffer clear resets logical contents", "[algorithm][fixed_ring_buffer]") {
  fixed_ring_buffer<int, 2> buffer;
  buffer.push(1);
  buffer.push(2);
  buffer.push(3);

  buffer.clear();

  CHECK(buffer.empty());
  CHECK(buffer.size() == 0);
  CHECK_FALSE(buffer.full());

  buffer.push(4);
  CHECK(buffer.size() == 1);
  CHECK(buffer.front() == 4);
  CHECK(buffer.back() == 4);
}

TEST_CASE("fixed_ring_buffer stores non-trivial values", "[algorithm][fixed_ring_buffer]") {
  fixed_ring_buffer<std::string, 2> buffer;
  buffer.push("alpha");
  buffer.push("beta");
  buffer.push("gamma");

  REQUIRE(buffer.size() == 2);
  CHECK(buffer.at(0) == "beta");
  CHECK(buffer.at(1) == "gamma");
}
