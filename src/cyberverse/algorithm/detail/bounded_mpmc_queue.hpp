/**
 * @file bounded_mpmc_queue.hpp
 * @author simon2077s (simon2077s@gmail.com)
 * @brief Fixed-capacity lock-free multi-producer multi-consumer queue
 * @version 0.1
 * @date 2026-10-28
 *
 * @copyright Copyright (c) 2026
 *
 */
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace cyberverse {
namespace algorithm {
namespace detail {

/**
 * @brief Fixed-capacity lock-free MPMC queue.
 *
 * This is the free-list primitive used by @ref sharded_queue. The storage is
 * allocated once at construction; the queue never allocates, blocks, or locks.
 *
 * The implementation uses the bounded MPMC algorithm from Dmitry Vyukov: every
 * slot stores a sequence number that encodes whether the slot is available for
 * an enqueue or a dequeue at the current position. Producers and consumers
 * advance their own position with compare-exchange and only claim a slot once
 * its sequence number matches the position, so no external memory reclamation
 * or ABA tagging is required.
 *
 * The capacity passed to the constructor is rounded up to a power of two.
 * `T` must be default-constructible because the slot array is allocated up
 * front; this queue is intended for pointer-sized handles.
 */
template <typename T>
class bounded_mpmc_queue {
public:
  /**
   * @brief Allocate storage for at least @p capacity elements.
   *
   * A capacity of zero is treated as one.
   */
  explicit bounded_mpmc_queue(std::size_t capacity)
      : slots_(std::make_unique<slot[]>(round_up_power_of_two(capacity))), mask_(round_up_power_of_two(capacity) - 1) {
    for (std::size_t i = 0; i < slot_count(); ++i) {
      slots_[i].sequence.store(i, std::memory_order_relaxed);
    }
  }

  bounded_mpmc_queue(const bounded_mpmc_queue&) = delete;
  bounded_mpmc_queue& operator=(const bounded_mpmc_queue&) = delete;
  bounded_mpmc_queue(bounded_mpmc_queue&&) = delete;
  bounded_mpmc_queue& operator=(bounded_mpmc_queue&&) = delete;
  ~bounded_mpmc_queue() = default;

  /**
   * @brief Number of elements the queue can hold at once.
   *
   * This is the constructor capacity rounded up to a power of two.
   */
  std::size_t capacity() const noexcept { return slot_count(); }

  /**
   * @brief Try to append one element.
   *
   * @return False when the queue is full. The call never blocks and never
   *         allocates.
   */
  bool try_enqueue(T value) noexcept {
    std::size_t position = enqueue_position_.load(std::memory_order_relaxed);
    for (;;) {
      slot& current = slots_[position & mask_];
      const std::size_t sequence = current.sequence.load(std::memory_order_acquire);
      const std::intptr_t difference = static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position);
      if (difference == 0) {
        if (enqueue_position_.compare_exchange_weak(
                position, position + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
          current.value = value;
          current.sequence.store(position + 1, std::memory_order_release);
          return true;
        }
      } else if (difference < 0) {
        return false;
      } else {
        position = enqueue_position_.load(std::memory_order_relaxed);
      }
    }
  }

  /**
   * @brief Try to remove one element.
   *
   * @param out Receives the element when the call returns true.
   * @return False when the queue is empty. The call never blocks and never
   *         allocates.
   */
  bool try_dequeue(T& out) noexcept {
    std::size_t position = dequeue_position_.load(std::memory_order_relaxed);
    for (;;) {
      slot& current = slots_[position & mask_];
      const std::size_t sequence = current.sequence.load(std::memory_order_acquire);
      const std::intptr_t difference = static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position + 1);
      if (difference == 0) {
        if (dequeue_position_.compare_exchange_weak(
                position, position + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
          out = current.value;
          current.sequence.store(position + mask_ + 1, std::memory_order_release);
          return true;
        }
      } else if (difference < 0) {
        return false;
      } else {
        position = dequeue_position_.load(std::memory_order_relaxed);
      }
    }
  }

private:
  struct slot {
    std::atomic<std::size_t> sequence{ 0 };
    T value{};
  };

  static std::size_t round_up_power_of_two(std::size_t capacity) noexcept {
    if (capacity <= 1) {
      return 1;
    }
    return std::bit_ceil(capacity);
  }

  std::size_t slot_count() const noexcept { return mask_ + 1; }

  std::unique_ptr<slot[]> slots_;
  std::size_t mask_ = 0;
  alignas(64) std::atomic<std::size_t> enqueue_position_{ 0 };
  alignas(64) std::atomic<std::size_t> dequeue_position_{ 0 };
};

} // namespace detail
} // namespace algorithm
} // namespace cyberverse
