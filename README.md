# algorithm

Cyberverse Engine Algorithm Library.

## sharded_queue

`cyberverse/algorithm/container/sharded_queue.hpp` provides a bounded,
multi-producer, single-consumer queue. Each writer owns a `producer` handle and
fills a private chunk. When a chunk is full and the writer starts the next one,
or when the writer flushes it, the producer publishes the chunk onto its own
lock-free stack; the single consumer drains every writer's stack, moves the items
out, and recycles the empty chunks through a lock-free chunk pool.

See [docs/shared_queue.md](docs/shared_queue.md) for a detailed Chinese guide
covering the design, per-function logic, and usage patterns.

```cpp
using command_queue = cyberverse::algorithm::sharded_queue<command, 256>;

auto queue = std::make_shared<command_queue>(/*chunk_pool_capacity=*/64,
                                             /*max_producer_count=*/8);

std::thread writer([queue] {
  const std::shared_ptr<command_queue::producer> producer = queue->acquire_producer();
  producer->emplace(/* command constructor arguments */);
  producer->flush(); // publish the partially filled chunk
});

std::vector<command> commands;
while (running) {
  queue->drain_into(commands); // reuses the vector allocation
  for (const command& command : commands) {
    execute(command);
  }
  commands.clear();
}
```

`multithread_sharded_queue<T, ChunkSize>` is a convenience wrapper that lazily
acquires one producer per calling thread from thread-local storage and releases
it when that thread exits:

```cpp
cyberverse::algorithm::multithread_sharded_queue<command, 256> shared(queue);
shared.push(command{});
shared.flush();
std::vector<command> commands = shared.drain();
```

Semantics:

- The chunk pool is preallocated. When every chunk is in use, pushes drop the
  item and increment `dropped_count()`; producers never block and never
  allocate.
- `drain()` and `drain_into()` only see chunks that have been published. A
  producer publishes its current chunk when it starts a new chunk, when it
  calls `producer::flush()`, when it observes a `request_flush_all()` request,
  or when it is destroyed.
- Items pushed by one producer are drained in push order. There is no ordering
  guarantee between different producers.
- Arithmetic on counters (`dropped_count`, `pending_chunks`) is relaxed; the
  values are exact once producers have stopped.
- Exactly one consumer thread may call `drain()` / `drain_into()`. Producer
  pushes are lock-free; a mutex is used only to register and unregister
  producers.
- The queue derives from `std::enable_shared_from_this` and must be owned by a
  `std::shared_ptr`; producers keep the queue alive.

### Flush requests

`sharded_queue::request_flush_all()` asks every producer to publish the chunk it
is currently filling. The request is cooperative and non-blocking: it does not
wake producer threads. Active producers check it at the start of every
`push`/`emplace`; an idle writer calls `producer::flush_if_requested()` (or
`multithread_sharded_queue::flush_if_requested()` for its thread-local producer).
An explicit `flush()` also acknowledges requests that were visible at its entry.

`request_flush_all()` itself never publishes data. The consumer must let
producers run their check (for example after a frame or thread barrier) before
draining; a producer blocked on an external condition variable must be woken by
the application.

```cpp
queue->request_flush_all();

// Active writers handle the request on their next push/emplace. An idle writer
// polls its own producer:
producer->flush_if_requested();
```

## fixed_ring_buffer

`cyberverse/algorithm/container/fixed_ring_buffer.hpp` is a fixed-capacity FIFO
with `Capacity` inline slots. `push` overwrites the oldest element when full;
elements are read in logical order (`at(0)` is the oldest, `at(size() - 1)` the
newest). It never allocates, never changes capacity, and provides no
synchronization.

```cpp
cyberverse::algorithm::fixed_ring_buffer<int, 4> buffer;
buffer.push(1);
buffer.push(2);
// buffer.at(0) == 1, buffer.back() == 2
```

## Build, test, and benchmark

Tests are built and run under `/tmp`, never inside the repository.

```bash
cmake -S . -B /tmp/cyberverse-algorithm-test
cmake --build /tmp/cyberverse-algorithm-test
ctest --test-dir /tmp/cyberverse-algorithm-test --output-on-failure
/tmp/cyberverse-algorithm-test/benchmark/cyberverse_algorithm_benchmarks
```
