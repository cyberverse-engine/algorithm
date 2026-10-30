/**
 * @file spsc_queue.hpp
 * @author simon2077s (simon2077s@gmail.com)
 * @brief Fixed-capacity single-producer/single-consumer queue
 * @version 0.1
 * @date 2026-10-30
 *
 * @copyright Copyright (c) 2026
 *
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace cyberverse {
namespace algorithm {

/**
 * @brief Fixed-capacity lock-free queue for one producer and one consumer.
 *
 * The producer calls try_push() and the consumer calls try_pop() from two
 * different threads. The queue never blocks and never allocates after
 * construction. It exposes at most capacity() - 1 queued elements; try_push()
 * returns false when full and leaves all retained elements unchanged.
 *
 * All operations are safe only with one producer and one consumer. The queue
 * does not provide multi-producer or multi-consumer semantics.
 *
 * @tparam T Element type; must be default constructible, copy assignable, and
 *           move assignable.
 */
template <typename T>
class spsc_queue {
  static_assert(std::is_default_constructible_v<T>, "spsc_queue requires a default-constructible T");
  static_assert(std::is_copy_assignable_v<T>, "spsc_queue requires a copy-assignable T");
  static_assert(std::is_move_assignable_v<T>, "spsc_queue requires a move-assignable T");

public:
  /**
   * @brief Construct a queue with fixed storage for @p capacity elements.
   *
   * @param capacity Total slots; must be at least 2.
   * @throws std::invalid_argument If capacity is less than 2.
   */
  explicit spsc_queue(std::size_t capacity) : buffer_(capacity) {
    if (capacity < 2) {
      throw std::invalid_argument("spsc_queue requires capacity >= 2");
    }
  }

  spsc_queue(const spsc_queue&) = delete;
  spsc_queue(spsc_queue&&) = delete;
  spsc_queue& operator=(const spsc_queue&) = delete;
  spsc_queue& operator=(spsc_queue&&) = delete;

  /**
   * @brief Try to push one value from the producer thread.
   *
   * @return false when the queue is full; the value is not overwritten.
   */
  bool try_push(const T& value) noexcept(std::is_nothrow_copy_assignable_v<T>) {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    const std::size_t next = (tail + 1) % buffer_.size();
    if (next == head_.load(std::memory_order_acquire)) {
      return false;
    }
    buffer_[tail] = value;
    tail_.store(next, std::memory_order_release);
    return true;
  }

  /**
   * @brief Try to pop one value from the consumer thread.
   *
   * @return false when the queue is empty.
   */
  bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    if (head == tail_.load(std::memory_order_acquire)) {
      return false;
    }
    out = std::move(buffer_[head]);
    head_.store((head + 1) % buffer_.size(), std::memory_order_release);
    return true;
  }

  /// @return Total slot count.
  [[nodiscard]] std::size_t capacity() const noexcept { return buffer_.size(); }

  /// @return Maximum number of queued elements (capacity - 1).
  [[nodiscard]] std::size_t usable_capacity() const noexcept { return buffer_.size() - 1; }

  /// @return Approximate queued element count.
  [[nodiscard]] std::size_t size() const noexcept {
    const std::size_t head = head_.load(std::memory_order_acquire);
    const std::size_t tail = tail_.load(std::memory_order_acquire);
    return tail >= head ? tail - head : buffer_.size() - head + tail;
  }

  /// @return Approximate empty state.
  [[nodiscard]] bool empty() const noexcept {
    return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
  }

private:
  std::vector<T> buffer_;
  alignas(64) std::atomic<std::size_t> head_{ 0 };
  alignas(64) std::atomic<std::size_t> tail_{ 0 };
};

} // namespace algorithm
} // namespace cyberverse
