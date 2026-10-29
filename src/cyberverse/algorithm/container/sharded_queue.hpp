/**
 * @file sharded_queue.hpp
 * @author simon2077s (simon2077s@gmail.com)
 * @brief Sharded multi-producer single-consumer queue
 * @version 0.1
 * @date 2026-10-28
 *
 * @copyright Copyright (c) 2026
 *
 */
#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cyberverse/algorithm/detail/bounded_mpmc_queue.hpp"

namespace cyberverse {
namespace algorithm {

template <typename T, std::size_t ChunkSize>
class sharded_queue;

template <typename T, std::size_t ChunkSize>
class multithread_sharded_queue;

/**
 * @brief Multi-producer, single-consumer queue with per-writer sharding.
 *
 * Every writer owns a @ref producer handle. A producer writes items into a
 * private, fixed-size @ref chunk. A chunk becomes visible to the consumer when
 * the producer fills it and starts the next chunk, or when the writer flushes
 * it; a full chunk that is still current is published by the next push, flush,
 * or producer destruction. Publishing is a lock-free compare-exchange on the
 * writer's own completion stack, so writers never contend with each other.
 * The single consumer drains every writer's published chunks in one pass,
 * moves the items into a caller-provided vector, and recycles the emptied
 * chunks through a bounded lock-free free list.
 *
 * The queue owns a fixed number of chunks. When every chunk is in use,
 * @ref producer::push and @ref producer::emplace drop the item and increment
 * @ref dropped_count; they never block and never allocate. The pool is created
 * in the constructor, so producer pushes do not allocate.
 *
 * Ordering: items pushed by one producer are drained in push order. Chunks are
 * published atomically, so a chunk that is still being filled is not visible to
 * the consumer; call @ref producer::flush to publish it. Chunks from different
 * producers have no relative order.
 *
 * Flush requests are cooperative. @ref request_flush_all increments a
 * generation counter; producers observe it on their next push or emplace, or
 * when the owning thread calls @ref producer::flush_if_requested. The queue
 * never wakes a producer thread, so an idle producer responds only when its
 * application loop polls for the request.
 *
 * Lifetime: a producer holds a `shared_ptr` to its queue, and the queue must be
 * owned by a `shared_ptr` (it derives from `std::enable_shared_from_this`).
 * @ref acquire_producer therefore returns `nullptr` only when all producer
 * slots are taken. The consumer must be the only thread that calls @ref drain
 * or @ref drain_into; every other member is safe to call concurrently.
 *
 * @tparam T Item type. It must be destructible and constructible into a
 *           `std::vector<T>`; `drain` moves items out of the chunks.
 * @tparam ChunkSize Number of items per chunk. Must be greater than zero.
 */
template <typename T, std::size_t ChunkSize>
class sharded_queue : public std::enable_shared_from_this<sharded_queue<T, ChunkSize>> {
public:
  static_assert(ChunkSize > 0, "sharded_queue requires a non-zero chunk size");

  /// Default number of chunks in the queue-owned pool.
  static constexpr std::size_t default_chunk_pool_capacity = 64;
  /// Default number of writer slots.
  static constexpr std::size_t default_max_producer_count = 64;

  class chunk;
  class producer;

  /**
   * @brief Create a queue with a preallocated chunk pool.
   *
   * @param chunk_pool_capacity Number of chunks to allocate. Values of zero are
   *        treated as one. This bounds the number of items that can be buffered
   *        before the producers start dropping.
   * @param max_producer_count Number of writer slots. Values of zero are treated
   *        as one. @ref acquire_producer returns `nullptr` once every slot is
   *        taken.
   */
  explicit sharded_queue(std::size_t chunk_pool_capacity = default_chunk_pool_capacity,
                         std::size_t max_producer_count = default_max_producer_count);
  ~sharded_queue() = default;

  sharded_queue(const sharded_queue&) = delete;
  sharded_queue& operator=(const sharded_queue&) = delete;
  sharded_queue(sharded_queue&&) = delete;
  sharded_queue& operator=(sharded_queue&&) = delete;

  /**
   * @brief Drain all currently published chunks into a new vector.
   *
   * Items are appended in chunk order and, within a chunk, in push order. Only
   * published chunks are visible. A producer publishes the chunk it is filling
   * either when it starts a new chunk or when it calls @ref producer::flush;
   * destroying the producer also publishes it.
   */
  std::vector<T> drain();

  /**
   * @brief Drain all currently published chunks, appending to @p out.
   *
   * The vector is not cleared. This overload lets a polling consumer reuse its
   * allocation across frames.
   */
  void drain_into(std::vector<T>& out);

  /**
   * @brief Number of items dropped because the queue could not accept them.
   *
   * A producer push is dropped when no chunk is available, and
   * @ref multithread_sharded_queue additionally drops when no producer slot is
   * available. The value is a relaxed snapshot; it is exact after all producers
   * stop.
   */
  std::uint64_t dropped_count() const noexcept { return dropped_count_.load(std::memory_order_relaxed); }

  /**
   * @brief Number of published chunks that have not been drained yet.
   *
   * The value is a relaxed snapshot taken while producers may be active. It is
   * zero after @ref drain or @ref drain_into returns when no producer publishes
   * concurrently.
   */
  std::size_t pending_chunks() const noexcept { return pending_chunk_count_.load(std::memory_order_relaxed); }

  /**
   * @brief Ask every producer to publish its current chunk.
   *
   * This is a cooperative, non-blocking request. It does not wake or interrupt
   * producer threads: each producer observes the request on its next @ref
   * producer::push, @ref producer::emplace, @ref producer::flush, or @ref
   * producer::flush_if_requested call, and publishes its buffered chunk if it
   * has one. Multiple requests are coalesced per producer.
   *
   * The consumer must not assume data is visible when this method returns.
   * Active producers respond on their next write; idle or blocked producers
   * must be polled or woken by the application.
   */
  void request_flush_all() noexcept;

  /**
   * @brief Number of chunks allocated for the queue.
   */
  std::size_t chunk_pool_capacity() const noexcept { return chunk_pool_capacity_; }

  /**
   * @brief Number of writer slots.
   */
  std::size_t max_producer_count() const noexcept { return max_producer_count_; }

  /**
   * @brief Acquire a writer handle.
   *
   * The returned producer keeps the queue alive. A producer is intended to be
   * kept for as long as a thread writes to the queue; for automatic per-thread
   * producers use @ref multithread_sharded_queue.
   *
   * @return `nullptr` when all writer slots are in use, or when the queue is not
   *         owned by a `std::shared_ptr` (in which case `shared_from_this`
   *         throws `std::bad_weak_ptr` instead of returning).
   */
  [[nodiscard]] std::shared_ptr<producer> acquire_producer();

private:
  friend class producer;
  template <typename, std::size_t>
  friend class multithread_sharded_queue;

  /// One writer slot. The queue owns the slot for its whole lifetime, so the
  /// consumer can safely scan it while a producer is being registered or
  /// destroyed. A slot is reused after its previous producer unregisters; any
  /// chunks left in `completed` are still drained by the consumer.
  struct alignas(64) writer_entry {
    std::atomic<chunk*> completed{ nullptr };
    producer* owner = nullptr;
  };

  static constexpr std::size_t normalize_capacity(std::size_t value) noexcept { return value == 0 ? 1 : value; }

  chunk* try_acquire_chunk() noexcept;
  void return_chunk(chunk* p_chunk) noexcept;
  void recycle_chunk(chunk* p_chunk) noexcept;
  void on_chunk_published() noexcept;
  void record_dropped(std::uint64_t count) noexcept;
  void unregister_producer(producer* p_producer) noexcept;
  chunk* collect_published_chunks() noexcept;

  std::size_t chunk_pool_capacity_;
  std::unique_ptr<chunk[]> chunks_;
  detail::bounded_mpmc_queue<chunk*> chunk_pool_;
  std::mutex registry_mutex_;
  std::size_t max_producer_count_;
  std::unique_ptr<writer_entry[]> writers_;
  std::size_t producer_count_ = 0;
  alignas(64) std::atomic<std::uint64_t> dropped_count_{ 0 };
  alignas(64) std::atomic<std::size_t> pending_chunk_count_{ 0 };
  alignas(64) std::atomic<std::uint64_t> flush_request_epoch_{ 0 };
};

/**
 * @brief Fixed-size storage block used by @ref sharded_queue.
 *
 * A chunk is always owned by the queue. While a producer fills it, the chunk's
 * `header.count` is written only by that producer. After publication the
 * consumer reads the count and moves the items out. Chunks are recycled after
 * the consumer has drained them.
 *
 * The public interface is deliberately low level; users normally interact with
 * @ref sharded_queue::producer and @ref sharded_queue.
 */
template <typename T, std::size_t ChunkSize>
class alignas(64) sharded_queue<T, ChunkSize>::chunk {
public:
  struct chunk_header {
    std::size_t count = 0;
    chunk* next = nullptr;
  };

  chunk() = default;
  ~chunk() { clear(); }

  chunk(const chunk&) = delete;
  chunk& operator=(const chunk&) = delete;
  chunk(chunk&&) = delete;
  chunk& operator=(chunk&&) = delete;

  bool empty() const noexcept { return header.count == 0; }
  bool full() const noexcept { return header.count == ChunkSize; }
  std::size_t size() const noexcept { return header.count; }

  /**
   * @brief Construct one item in place.
   *
   * @return False when the chunk is already full. If the item constructor
   *         throws, the chunk is unchanged.
   */
  template <typename U>
  bool try_push(U&& item) {
    return try_emplace(std::forward<U>(item));
  }

  /**
   * @brief Construct one item in place from @p args.
   *
   * @return False when the chunk is already full. If the item constructor
   *         throws, the chunk is unchanged.
   */
  template <typename... Args>
  bool try_emplace(Args&&... args) {
    if (full()) {
      return false;
    }
    std::construct_at(storage_at(header.count), std::forward<Args>(args)...);
    ++header.count;
    return true;
  }

  /**
   * @brief Move every item into @p out and destroy the sources.
   *
   * If a move or the vector allocation throws, the remaining items are
   * destroyed, the chunk is left empty, and the exception is rethrown. Items
   * already appended to @p out stay there.
   */
  void drain_into(std::vector<T>& out) {
    std::size_t index = 0;
    try {
      for (; index < header.count; ++index) {
        out.emplace_back(std::move(*item_at(index)));
        std::destroy_at(item_at(index));
      }
    } catch (...) {
      destroy_items(index);
      header.count = 0;
      throw;
    }
    header.count = 0;
  }

  /**
   * @brief Destroy all items without moving them out.
   */
  void clear() noexcept {
    destroy_items(0);
    header.count = 0;
  }

  chunk_header header;

private:
  struct alignas(T) item_slot {
    std::byte bytes[sizeof(T)];
  };

  T* storage_at(std::size_t index) noexcept { return reinterpret_cast<T*>(storage_[index].bytes); }
  T* item_at(std::size_t index) noexcept { return std::launder(storage_at(index)); }

  void destroy_items(std::size_t first) noexcept {
    for (std::size_t index = first; index < header.count; ++index) {
      std::destroy_at(item_at(index));
    }
  }

  std::array<item_slot, ChunkSize> storage_;
};

/**
 * @brief Writer handle for @ref sharded_queue.
 *
 * A producer is not thread-safe: exactly one thread may push through it at a
 * time. The destructor flushes the partially filled chunk and unregisters the
 * writer, so a producer may be released at any point without losing buffered
 * items. Publishing and chunk acquisition are lock-free; the only lock in the
 * queue is taken when a writer is registered or unregistered.
 */
template <typename T, std::size_t ChunkSize>
class sharded_queue<T, ChunkSize>::producer {
public:
  using queue_type = sharded_queue<T, ChunkSize>;
  using chunk_type = typename queue_type::chunk;

  ~producer();

  producer(const producer&) = delete;
  producer& operator=(const producer&) = delete;
  producer(producer&&) = delete;
  producer& operator=(producer&&) = delete;

  /**
   * @brief Push one item, publishing the current chunk first when it is full.
   *
   * Before writing, this checks for a pending @ref
   * sharded_queue::request_flush_all request and publishes the current chunk if
   * one is pending. The item passed to this call is written after that check,
   * so it is not part of the flushed chunk.
   *
   * If no chunk is available, the item is dropped and
   * @ref sharded_queue::dropped_count is incremented.
   */
  template <typename U>
    requires(std::is_same_v<std::remove_cvref_t<U>, T>)
  void push(U&& item);

  /**
   * @brief Construct one item in place, publishing the current chunk first when
   *        it is full.
   *
   * Before writing, this checks for a pending @ref
   * sharded_queue::request_flush_all request and publishes the current chunk if
   * one is pending. The item passed to this call is written after that check,
   * so it is not part of the flushed chunk.
   *
   * If no chunk is available, the item is dropped and
   * @ref sharded_queue::dropped_count is incremented.
   */
  template <typename... Args>
  void emplace(Args&&... args);

  /**
   * @brief Take all chunks published by this producer so far.
   *
   * This is the low-level primitive behind @ref sharded_queue::drain. The
   * caller becomes responsible for draining each chunk and returning it with
   * @ref reuse_chunk. Most consumers should use @ref sharded_queue::drain or
   * @ref sharded_queue::drain_into instead.
   */
  chunk_type* take_completed() noexcept { return entry_->completed.exchange(nullptr, std::memory_order_acquire); }

  /**
   * @brief Return a chunk obtained from @ref take_completed to the queue pool.
   *
   * The chunk is cleared first, so any items still inside it are destroyed.
   */
  void reuse_chunk(chunk_type* p_chunk);

  /**
   * @brief Publish the partially filled chunk.
   *
   * After this call the chunk's items become visible to the consumer. A
   * producer may keep writing into a new chunk afterwards. Flushing when no
   * items are buffered publishes nothing.
   *
   * A flush also records any request_flush_all() generation that was visible at
   * entry, so an explicit flush satisfies those requests.
   */
  void flush();

  /**
   * @brief Check for and handle a pending request_flush_all() request.
   *
   * If a newer request generation is visible, publishes the current chunk when
   * it contains items and records the generation. The method returns true when
   * a new request was observed, even when no items were buffered. Multiple
   * requests observed together are coalesced into one call.
   *
   * This lets an idle writer respond without pushing a new item. An empty
   * current chunk is kept for reuse; it is not returned to the pool. Push and
   * emplace call this automatically at their entry.
   *
   * Must be called on the thread that owns this producer.
   */
  bool flush_if_requested() noexcept;

private:
  friend class sharded_queue;

  using entry_type = typename queue_type::writer_entry;

  producer(queue_type& q, entry_type& entry);

  bool ensure_writable_chunk();
  void publish_current() noexcept;
  void rotate();
  void record_drop() noexcept;

  std::shared_ptr<queue_type> p_queue_;
  entry_type* entry_ = nullptr;
  chunk_type* current_ = nullptr;
  std::uint64_t observed_flush_epoch_ = 0;
};

/**
 * @brief Convenience wrapper that keeps one producer per calling thread.
 *
 * The wrapper owns a shared queue and lazily acquires a producer the first time
 * a thread pushes. The producer is cached in thread-local storage and is
 * flushed and unregistered when that thread exits, so callers do not have to
 * manage producer handles themselves.
 *
 * The wrapper is safe to share across threads: each thread writes through its
 * own producer while one consumer thread calls @ref drain or @ref drain_into.
 * Destroy the wrapper after the writer threads have stopped; writers on other
 * threads hold their producer (and therefore the underlying queue) until their
 * thread exits.
 *
 * @tparam T Item type, forwarded to @ref sharded_queue.
 * @tparam ChunkSize Number of items per chunk, forwarded to @ref sharded_queue.
 */
template <typename T, std::size_t ChunkSize>
class multithread_sharded_queue {
public:
  using queue_type = sharded_queue<T, ChunkSize>;
  using producer_type = typename queue_type::producer;

  /**
   * @brief Wrap an existing queue.
   *
   * @param queue Queue shared by every calling thread. Must not be null.
   */
  explicit multithread_sharded_queue(std::shared_ptr<queue_type> queue);
  ~multithread_sharded_queue();

  multithread_sharded_queue(const multithread_sharded_queue&) = delete;
  multithread_sharded_queue& operator=(const multithread_sharded_queue&) = delete;
  multithread_sharded_queue(multithread_sharded_queue&&) = delete;
  multithread_sharded_queue& operator=(multithread_sharded_queue&&) = delete;

  /**
   * @brief Push through the calling thread's producer.
   *
   * The producer is created on first use. If all writer slots are taken, the
   * item is dropped and @ref dropped_count is incremented.
   */
  template <typename U>
    requires(std::is_same_v<std::remove_cvref_t<U>, T>)
  void push(U&& item) {
    producer_type* local = local_producer();
    if (local == nullptr) {
      queue_->record_dropped(1);
      return;
    }
    local->push(std::forward<U>(item));
  }

  /**
   * @brief Emplace through the calling thread's producer.
   *
   * If all writer slots are taken, the item is dropped and
   * @ref dropped_count is incremented.
   */
  template <typename... Args>
  void emplace(Args&&... args) {
    producer_type* local = local_producer();
    if (local == nullptr) {
      queue_->record_dropped(1);
      return;
    }
    local->emplace(std::forward<Args>(args)...);
  }

  /// Flush the calling thread's producer, if it has one.
  void flush() {
    if (producer_type* local = find_local_producer()) {
      local->flush();
    }
  }

  /// Ask every producer to publish its current chunk.
  void request_flush_all() noexcept { queue_->request_flush_all(); }

  /**
   * @brief Check for and handle a pending flush request for this thread's
   *        producer.
   *
   * Does not create a producer. Returns false when the calling thread has no
   * producer, because there is nothing buffered on this thread to publish.
   */
  bool flush_if_requested() noexcept {
    if (producer_type* local = find_local_producer()) {
      return local->flush_if_requested();
    }
    return false;
  }

  /// Drain all published chunks into a new vector.
  std::vector<T> drain() { return queue_->drain(); }

  /// Drain all published chunks, appending to @p out.
  void drain_into(std::vector<T>& out) { queue_->drain_into(out); }

  /// @copydoc sharded_queue::dropped_count
  std::uint64_t dropped_count() const noexcept { return queue_->dropped_count(); }

  /// @copydoc sharded_queue::pending_chunks
  std::size_t pending_chunks() const noexcept { return queue_->pending_chunks(); }

  /// The wrapped queue.
  const std::shared_ptr<queue_type>& queue() const noexcept { return queue_; }

private:
  struct cache_entry {
    std::uint64_t id = 0;
    std::shared_ptr<producer_type> producer;
  };

  static std::uint64_t next_id() noexcept;
  static std::vector<cache_entry>& thread_cache();
  producer_type* find_local_producer() noexcept;
  producer_type* local_producer();
  void clear_thread_cache() noexcept;

  std::shared_ptr<queue_type> queue_;
  std::uint64_t id_;
};

// -----------------------------------------------------------------------------
// sharded_queue
// -----------------------------------------------------------------------------

template <typename T, std::size_t ChunkSize>
sharded_queue<T, ChunkSize>::sharded_queue(std::size_t chunk_pool_capacity, std::size_t max_producer_count)
    : chunk_pool_capacity_(normalize_capacity(chunk_pool_capacity))
    , chunks_(std::make_unique<chunk[]>(chunk_pool_capacity_))
    , chunk_pool_(chunk_pool_capacity_)
    , max_producer_count_(normalize_capacity(max_producer_count))
    , writers_(std::make_unique<writer_entry[]>(max_producer_count_)) {
  for (std::size_t i = 0; i < chunk_pool_capacity_; ++i) {
    const bool inserted = chunk_pool_.try_enqueue(&chunks_[i]);
    assert(inserted);
    (void)inserted;
  }
}

template <typename T, std::size_t ChunkSize>
std::vector<T> sharded_queue<T, ChunkSize>::drain() {
  std::vector<T> items;
  const std::size_t pending = pending_chunk_count_.load(std::memory_order_relaxed);
  items.reserve(pending * ChunkSize);
  drain_into(items);
  return items;
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::drain_into(std::vector<T>& out) {
  chunk* chain = collect_published_chunks();
  while (chain != nullptr) {
    chunk* next = chain->header.next;
    chain->header.next = nullptr;
    try {
      chain->drain_into(out);
    } catch (...) {
      chain->clear();
      recycle_chunk(chain);
      while (next != nullptr) {
        chunk* remaining = next->header.next;
        next->clear();
        recycle_chunk(next);
        next = remaining;
      }
      throw;
    }
    recycle_chunk(chain);
    chain = next;
  }
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::request_flush_all() noexcept {
  flush_request_epoch_.fetch_add(1, std::memory_order_release);
}

template <typename T, std::size_t ChunkSize>
std::shared_ptr<typename sharded_queue<T, ChunkSize>::producer> sharded_queue<T, ChunkSize>::acquire_producer() {
  std::lock_guard<std::mutex> lock(registry_mutex_);
  if (producer_count_ >= max_producer_count_) {
    return nullptr;
  }

  writer_entry* entry = nullptr;
  for (std::size_t i = 0; i < max_producer_count_; ++i) {
    if (writers_[i].owner == nullptr) {
      entry = &writers_[i];
      break;
    }
  }
  if (entry == nullptr) {
    return nullptr;
  }

  std::shared_ptr<producer> result(new producer(*this, *entry));
  entry->owner = result.get();
  ++producer_count_;
  return result;
}

template <typename T, std::size_t ChunkSize>
typename sharded_queue<T, ChunkSize>::chunk* sharded_queue<T, ChunkSize>::try_acquire_chunk() noexcept {
  chunk* acquired = nullptr;
  if (!chunk_pool_.try_dequeue(acquired)) {
    return nullptr;
  }
  acquired->header.count = 0;
  acquired->header.next = nullptr;
  return acquired;
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::return_chunk(chunk* p_chunk) noexcept {
  assert(p_chunk != nullptr);
  p_chunk->header.next = nullptr;
  // A concurrent dequeue may have reserved its ring position but not yet
  // released the slot for the next enqueue. The chunk is logically free, so
  // wait for that dequeue to finish instead of losing it.
  while (!chunk_pool_.try_enqueue(p_chunk)) {
    std::this_thread::yield();
  }
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::recycle_chunk(chunk* p_chunk) noexcept {
  assert(p_chunk != nullptr);
  assert(p_chunk->empty());
  const std::size_t previous = pending_chunk_count_.fetch_sub(1, std::memory_order_relaxed);
  assert(previous > 0);
  (void)previous;
  return_chunk(p_chunk);
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::on_chunk_published() noexcept {
  pending_chunk_count_.fetch_add(1, std::memory_order_relaxed);
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::record_dropped(std::uint64_t count) noexcept {
  dropped_count_.fetch_add(count, std::memory_order_relaxed);
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::unregister_producer(producer* p_producer) noexcept {
  std::lock_guard<std::mutex> lock(registry_mutex_);
  if (p_producer == nullptr || p_producer->entry_ == nullptr) {
    return;
  }
  if (p_producer->entry_->owner == p_producer) {
    p_producer->entry_->owner = nullptr;
    assert(producer_count_ > 0);
    --producer_count_;
  }
}

template <typename T, std::size_t ChunkSize>
typename sharded_queue<T, ChunkSize>::chunk* sharded_queue<T, ChunkSize>::collect_published_chunks() noexcept {
  chunk* chain = nullptr;
  chunk* chain_tail = nullptr;

  for (std::size_t i = 0; i < max_producer_count_; ++i) {
    chunk* published = writers_[i].completed.exchange(nullptr, std::memory_order_acquire);
    if (published == nullptr) {
      continue;
    }

    // Writers publish a LIFO stack. Reverse it so the consumer sees the chunks
    // in publication order before splicing the chain into the result.
    chunk* published_tail = published;
    chunk* reversed = nullptr;
    while (published != nullptr) {
      chunk* next = published->header.next;
      published->header.next = reversed;
      reversed = published;
      published = next;
    }

    if (chain == nullptr) {
      chain = reversed;
    } else {
      chain_tail->header.next = reversed;
    }
    chain_tail = published_tail;
  }

  return chain;
}

// -----------------------------------------------------------------------------
// sharded_queue::producer
// -----------------------------------------------------------------------------

template <typename T, std::size_t ChunkSize>
sharded_queue<T, ChunkSize>::producer::producer(queue_type& q, entry_type& entry)
    : p_queue_(q.shared_from_this())
    , entry_(&entry)
    , observed_flush_epoch_(q.flush_request_epoch_.load(std::memory_order_acquire)) {}

template <typename T, std::size_t ChunkSize>
sharded_queue<T, ChunkSize>::producer::~producer() {
  flush();
  if (p_queue_ != nullptr) {
    p_queue_->unregister_producer(this);
  }
}

template <typename T, std::size_t ChunkSize>
template <typename U>
  requires(std::is_same_v<std::remove_cvref_t<U>, T>)
void sharded_queue<T, ChunkSize>::producer::push(U&& item) {
  flush_if_requested();
  if (!ensure_writable_chunk()) {
    record_drop();
    return;
  }
  const bool inserted = current_->try_push(std::forward<U>(item));
  assert(inserted);
  (void)inserted;
}

template <typename T, std::size_t ChunkSize>
template <typename... Args>
void sharded_queue<T, ChunkSize>::producer::emplace(Args&&... args) {
  flush_if_requested();
  if (!ensure_writable_chunk()) {
    record_drop();
    return;
  }
  const bool inserted = current_->try_emplace(std::forward<Args>(args)...);
  assert(inserted);
  (void)inserted;
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::producer::reuse_chunk(chunk_type* p_chunk) {
  if (p_chunk == nullptr) {
    return;
  }
  p_chunk->clear();
  p_queue_->recycle_chunk(p_chunk);
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::producer::flush() {
  const std::uint64_t epoch = p_queue_->flush_request_epoch_.load(std::memory_order_acquire);
  if (current_ != nullptr) {
    publish_current();
  }
  observed_flush_epoch_ = epoch;
}

template <typename T, std::size_t ChunkSize>
bool sharded_queue<T, ChunkSize>::producer::flush_if_requested() noexcept {
  const std::uint64_t epoch = p_queue_->flush_request_epoch_.load(std::memory_order_acquire);
  if (epoch == observed_flush_epoch_) {
    return false;
  }
  if (current_ != nullptr && !current_->empty()) {
    publish_current();
  }
  observed_flush_epoch_ = epoch;
  return true;
}

template <typename T, std::size_t ChunkSize>
bool sharded_queue<T, ChunkSize>::producer::ensure_writable_chunk() {
  if (current_ != nullptr && !current_->full()) {
    return true;
  }
  if (current_ != nullptr) {
    rotate();
  } else {
    current_ = p_queue_->try_acquire_chunk();
  }
  return current_ != nullptr;
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::producer::publish_current() noexcept {
  chunk_type* finished = current_;
  current_ = nullptr;
  if (finished == nullptr) {
    return;
  }
  if (finished->empty()) {
    p_queue_->return_chunk(finished);
    return;
  }

  // Account for the chunk before making it visible: the consumer decrements the
  // counter when it recycles the chunk, so it must never observe the chunk
  // without the increment.
  p_queue_->on_chunk_published();

  // The comparison loop is required because the consumer may exchange the
  // completion stack between the load and the store. A plain store there would
  // re-link an already drained chain into the new chunk.
  chunk_type* head = entry_->completed.load(std::memory_order_relaxed);
  for (;;) {
    finished->header.next = head;
    if (entry_->completed.compare_exchange_weak(head, finished, std::memory_order_release, std::memory_order_relaxed)) {
      return;
    }
  }
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::producer::rotate() {
  if (current_ == nullptr || current_->empty()) {
    return;
  }
  publish_current();
  current_ = p_queue_->try_acquire_chunk();
}

template <typename T, std::size_t ChunkSize>
void sharded_queue<T, ChunkSize>::producer::record_drop() noexcept {
  p_queue_->record_dropped(1);
}

// -----------------------------------------------------------------------------
// multithread_sharded_queue
// -----------------------------------------------------------------------------

template <typename T, std::size_t ChunkSize>
multithread_sharded_queue<T, ChunkSize>::multithread_sharded_queue(std::shared_ptr<queue_type> queue)
    : queue_(std::move(queue)), id_(next_id()) {
  if (queue_ == nullptr) {
    throw std::invalid_argument("multithread_sharded_queue requires a non-null queue");
  }
}

template <typename T, std::size_t ChunkSize>
multithread_sharded_queue<T, ChunkSize>::~multithread_sharded_queue() {
  clear_thread_cache();
}

template <typename T, std::size_t ChunkSize>
std::uint64_t multithread_sharded_queue<T, ChunkSize>::next_id() noexcept {
  static std::atomic<std::uint64_t> counter{ 0 };
  return counter.fetch_add(1, std::memory_order_relaxed);
}

template <typename T, std::size_t ChunkSize>
std::vector<typename multithread_sharded_queue<T, ChunkSize>::cache_entry>&
multithread_sharded_queue<T, ChunkSize>::thread_cache() {
  thread_local std::vector<cache_entry> cache;
  return cache;
}

template <typename T, std::size_t ChunkSize>
typename multithread_sharded_queue<T, ChunkSize>::producer_type*
multithread_sharded_queue<T, ChunkSize>::find_local_producer() noexcept {
  for (cache_entry& entry : thread_cache()) {
    if (entry.id == id_) {
      return entry.producer.get();
    }
  }
  return nullptr;
}

template <typename T, std::size_t ChunkSize>
typename multithread_sharded_queue<T, ChunkSize>::producer_type*
multithread_sharded_queue<T, ChunkSize>::local_producer() {
  if (producer_type* cached = find_local_producer()) {
    return cached;
  }

  std::shared_ptr<producer_type> acquired = queue_->acquire_producer();
  if (acquired == nullptr) {
    return nullptr;
  }

  std::vector<cache_entry>& cache = thread_cache();
  cache.push_back(cache_entry{ id_, std::move(acquired) });
  return cache.back().producer.get();
}

template <typename T, std::size_t ChunkSize>
void multithread_sharded_queue<T, ChunkSize>::clear_thread_cache() noexcept {
  std::vector<cache_entry>& cache = thread_cache();
  for (auto it = cache.begin(); it != cache.end(); ++it) {
    if (it->id == id_) {
      cache.erase(it);
      return;
    }
  }
}

} // namespace algorithm
} // namespace cyberverse
