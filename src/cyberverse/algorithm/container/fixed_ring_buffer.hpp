/**
 * @file fixed_ring_buffer.hpp
 * @author simon2077s (simon2077s@gmail.com)
 * @brief Fixed-capacity ring buffer with compile-time storage
 * @version 0.1
 * @date 2026-10-29
 *
 * @copyright Copyright (c) 2026
 *
 */
#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace cyberverse {
namespace algorithm {

/**
 * @brief Fixed-capacity FIFO that overwrites the oldest element when full.
 *
 * The buffer owns `Capacity` elements in a `std::array`, so it never allocates
 * and never changes capacity. Elements are addressed in logical order:
 * index 0 is the oldest retained element and `size() - 1` is the newest.
 *
 * The buffer is not synchronized. It is intended for a single writer/reader
 * thread, or for callers that provide their own synchronization.
 *
 * @tparam T Stored value type. It must be default constructible (the array owns
 *           all slots), move assignable, and destructible.
 * @tparam Capacity Number of stored elements. Must be greater than zero.
 */
template <typename T, std::size_t Capacity>
class fixed_ring_buffer {
  static_assert(Capacity > 0, "fixed_ring_buffer requires a non-zero capacity");
  static_assert(std::is_default_constructible_v<T>, "fixed_ring_buffer<T, N> requires a default-constructible T");
  static_assert(std::is_move_assignable_v<T>, "fixed_ring_buffer<T, N> requires a move-assignable T");

public:
  /// Number of elements the buffer can retain.
  static constexpr std::size_t capacity = Capacity;

  /**
   * @brief Append a value, overwriting the oldest value when the buffer is full.
   */
  void push(T value) {
    if (size_ < Capacity) {
      data_[(head_ + size_) % Capacity] = std::move(value);
      ++size_;
      return;
    }

    data_[head_] = std::move(value);
    head_ = (head_ + 1) % Capacity;
  }

  /**
   * @brief Access an element in logical order.
   * @param index 0 is the oldest element; `size() - 1` is the newest.
   *
   * The caller must ensure `index < size()`.
   */
  T& at(std::size_t index) {
    assert(index < size_);
    return data_[(head_ + index) % Capacity];
  }

  /// @copydoc at(std::size_t)
  const T& at(std::size_t index) const {
    assert(index < size_);
    return data_[(head_ + index) % Capacity];
  }

  T& operator[](std::size_t index) { return at(index); }
  const T& operator[](std::size_t index) const { return at(index); }

  /// @brief Oldest retained element. The buffer must not be empty.
  T& front() { return at(0); }
  const T& front() const { return at(0); }

  /// @brief Newest retained element. The buffer must not be empty.
  T& back() { return at(size_ - 1); }
  const T& back() const { return at(size_ - 1); }

  std::size_t size() const noexcept { return size_; }

  bool empty() const noexcept { return size_ == 0; }

  bool full() const noexcept { return size_ == Capacity; }

  /**
   * @brief Drop the logical contents.
   *
   * The elements remain constructed in the array but are no longer visible; a
   * later `push` overwrites them.
   */
  void clear() noexcept {
    head_ = 0;
    size_ = 0;
  }

private:
  std::array<T, Capacity> data_{};
  std::size_t head_ = 0;
  std::size_t size_ = 0;
};

} // namespace algorithm
} // namespace cyberverse
