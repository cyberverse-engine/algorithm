/**
 * @file triple_buffer.hpp
 * @author simon2077s (simon2077s@gmail.com)
 * @brief Single-writer/single-reader lock-free triple buffer
 * @version 0.1
 * @date 2026-10-30
 *
 * @copyright Copyright (c) 2026
 *
 */
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace cyberverse {
namespace algorithm {

/**
 * @brief Lock-free single-writer/single-reader latest-value buffer.
 *
 * The writer fills write_slot() and calls publish(). The reader calls read() to
 * obtain the latest published value. Three slots guarantee that the writer
 * never overwrites the slot currently held by the reader. No locks or
 * allocations are used after construction.
 *
 * The reference returned by read() remains valid until the next read() on the
 * same reader thread. Only one writer and one reader are supported.
 *
 * @tparam T Value type; must be default constructible.
 */
template <typename T>
class triple_buffer {
  static_assert(std::is_default_constructible_v<T>, "triple_buffer requires a default-constructible T");

public:
  triple_buffer() = default;

  triple_buffer(const triple_buffer&) = delete;
  triple_buffer(triple_buffer&&) = delete;
  triple_buffer& operator=(const triple_buffer&) = delete;
  triple_buffer& operator=(triple_buffer&&) = delete;

  /// @brief Writer-owned slot to fill before publish().
  [[nodiscard]] T& write_slot() noexcept { return slots_[write_index_]; }

  /// @brief Publish the current write slot and make it readable.
  void publish() noexcept {
    slot_generations_[write_index_] = ++write_generation_;
    const std::size_t old_ready = ready_.exchange(write_index_, std::memory_order_acq_rel);
    write_index_ = old_ready;
    published_generation_.store(write_generation_, std::memory_order_release);
  }

  /**
   * @brief Obtain the latest published value on the reader thread.
   *
   * The returned reference is valid until the next read() on the same thread.
   */
  [[nodiscard]] const T& read() noexcept {
    if (published_generation_.load(std::memory_order_acquire) != read_generation_) {
      const std::size_t new_read = ready_.exchange(read_index_, std::memory_order_acq_rel);
      read_index_ = new_read;
      read_generation_ = slot_generations_[read_index_];
    }
    return slots_[read_index_];
  }

private:
  std::array<T, 3> slots_{};
  std::array<std::uint64_t, 3> slot_generations_{};
  alignas(64) std::atomic<std::size_t> ready_{ 0 };
  alignas(64) std::atomic<std::uint64_t> published_generation_{ 0 };
  std::size_t read_index_ = 1;
  std::size_t write_index_ = 2;
  std::uint64_t write_generation_ = 0;
  std::uint64_t read_generation_ = 0;
};

} // namespace algorithm
} // namespace cyberverse
