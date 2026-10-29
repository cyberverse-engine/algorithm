# sharded_queue 原理、函数逻辑与用法

> 实现类型名为 `sharded_queue`，本文所在文件名为 `docs/shared_queue.md`。
>
> 头文件：[sharded_queue.hpp](../src/cyberverse/algorithm/container/sharded_queue.hpp)  
> 内部空闲链表：[bounded_mpmc_queue.hpp](../src/cyberverse/algorithm/detail/bounded_mpmc_queue.hpp)  
> 测试：[sharded_queue_test.cpp](../test/sharded_queue_test.cpp)  
> 基准测试：[sharded_queue_benchmark.cpp](../benchmark/sharded_queue_benchmark.cpp)

一句话总结：**生产者按线程分片写 chunk，chunk 写满后开始下一个或显式 flush 时无锁发布；单消费者一次取走所有已发布 chunk，把 item 移入 vector 并回收 chunk。** 队列有界、写入不阻塞、写入路径不分配。

## 1. 概览

`sharded_queue<T, ChunkSize>` 是一个**有界、多生产者、单消费者（MPSC）**队列，写入路径按生产者分片，消费者一次取走所有已发布的 chunk。类型位于
`cyberverse::algorithm` 命名空间，头文件为
`cyberverse/algorithm/container/sharded_queue.hpp`，也可以包含伞形头文件
`cyberverse/algorithm.hpp`。

| 特性 | 说明 |
| --- | --- |
| 并发模型 | 多生产者、单消费者 |
| 阻塞行为 | 永不阻塞 |
| 容量 | 有界：固定 chunk 池 |
| 写入分配 | 不分配内存 |
| 生产者锁 | 无；只在 producer 注册/注销时使用 mutex |
| 顺序 | 单个 producer 内严格按写入顺序 |
| 池耗尽 | 丢弃 item，累加 `dropped_count()` |
| 可见性 | 只有已发布的 chunk 对消费者可见 |

适用场景：

- 多线程产生事件、日志、命令，单线程集中消费；
- 游戏或引擎帧循环中，各 worker 线程提交任务，主线程每帧统一取出；
- 希望在写入热路径避免 per-item 原子操作、锁竞争和动态分配的场景。

不适用场景：

- 需要阻塞等待或背压反馈的队列（本队列池耗尽直接丢弃）；
- 多消费者并发消费；
- 需要无界缓存、不允许丢弃的场景；
- 希望把同一个 `producer` 跨线程共享写入的场景。

## 2. 设计目标与取舍

### 2.1 为什么按生产者分片

如果所有生产者把 item 直接压入同一个原子栈或原子队列，每写一个 item 都要对同一个 cache line 做 CAS：

- 高并发时 cache line 在多个核心之间反复弹跳；
- 生产者越多，单次写入越慢；
- 每个 item 一次原子操作，吞吐受限于共享原子变量。

分片后的做法是：

1. 每个 producer 拥有自己的 `current_` chunk；
2. item 先写入 chunk 的普通存储，不产生任何原子操作；
3. chunk 写满后，生产者开始写下一个 chunk（下一次 push 时触发）或显式 `flush()`
   时，才把整个 chunk 发布到该 producer 自己的 `completed` 链；
4. 每次发布只做一次 CAS，成本被 `ChunkSize` 个 item 摊销。

### 2.2 为什么有界并丢弃

chunk 池在队列构造时一次性分配，运行期不再分配：

- 写入路径没有 `new`/`malloc`，延迟稳定；
- 内存占用可预测；
- 代价是池耗尽时必须做选择。本实现选择“丢弃新 item + 计数”，而不是阻塞生产者或无限扩容。

如果业务不能接受丢弃，需要在队列之外实现背压，例如消费者加快 drain、扩大池容量，或在上层使用有界信号量。

### 2.3 为什么只允许一个消费者

单消费者是一个重要简化：

- 不需要多个消费者竞争同一条 completed 链；
- `pending_chunks`、槽位扫描、chunk 回收都不需要消费者之间的同步；
- 消费者可以独占地把 chunk 链反转、拼接并回收。

需要并行消费时，应创建多个队列，让每个消费者独占一个队列，而不是让多个线程同时 drain 同一个队列。

## 3. 整体结构

```text
                    sharded_queue<T, ChunkSize>
        +-------------------------------------------------------+
        |  chunk_pool : bounded_mpmc_queue<chunk*>              |
        |              固定容量的无锁空闲链表                    |
        |                                                       |
        |  writers : writer_entry[max_producer_count]           |
        |            每个槽位一条 completed 链（至少一个槽位）    |
        |                                                       |
        |  dropped_count / pending_chunks                       |
        +-------------------------------------------------------+
              ^ dequeue                        ^ scan + exchange
              |                                |
      producer.current_  -- publish(CAS) -->  writer_entry.completed
              |                                |
              +-------- 写满后的下一次 push / flush ---+
                                               |
                                        consumer drain
                                               |
                           chunk -> 移出 item -> vector
                                               |
                           chunk -> pending-- -> chunk_pool
```

生产者与消费者的完整数据流：

```text
生产者线程 (N 个)                                  消费者线程 (1 个)
-----------------                                  -----------------
producer::push
  |
  +-- current_ 未满 ---------------------> 写入 chunk storage
  |
  +-- current_ 已满
        |
        +-- publish_current()
              pending_chunks++
              CAS 挂到 writer_entry.completed
              |
              +------------------------------> collect_published_chunks()
                                                 扫描所有 writer_entry
                                                 exchange 整条链
                                                 每条链 LIFO -> FIFO
                                                 拼接成一条链
                                                       |
                                                       v
                                                 drain_into(out)
                                                 逐个 chunk 移出 item
                                                 回收 chunk
                                                   pending_chunks--
                                                   enqueue 回 chunk_pool
        <----------------------------------------------+
        try_acquire_chunk() 从池中取新 chunk
```

## 4. 核心数据结构

### 4.1 `chunk`

`chunk` 是固定大小的存储块，由队列持有，不归 producer 所有。

```cpp
struct chunk_header {
  std::size_t count = 0; // 已构造的 item 数
  chunk* next = nullptr; // completed 链或临时链的 next
};
```

每个 chunk 包含：

- `header`：item 数量与链指针；
- `storage_`：`ChunkSize` 个按 `T` 对齐的原始字节槽；
- 通过 placement new (`std::construct_at`) 原地构造 item；
- 通过 `std::destroy_at` 销毁 item；
- 使用 `std::launder` 安全地取得构造后的对象指针。

设计要点：

- 不要求 `T` 可默认构造，因为存储区是未初始化的原始字节；
- 支持 move-only 类型；
- chunk 整体 `alignas(64)`，减少相邻 chunk 之间的伪共享；
- `ChunkSize` 在编译期确定，队列的 item 容量上限约为
  `chunk_pool_capacity * ChunkSize`（实际可用量还受仍在 producer 手里的空 chunk 影响）。

### 4.2 `writer_entry`

每个 producer 对应一个队列持有的写者槽位：

```cpp
struct alignas(64) writer_entry {
  std::atomic<chunk*> completed{ nullptr };
  producer* owner = nullptr; // 只在 registry_mutex_ 下读写
};
```

这个结构是并发安全的关键：

- **槽位属于队列，生命周期覆盖整个队列**，消费者扫描槽位时不会访问 `producer` 对象；
- 即使 producer 正在析构、槽位正在被新 producer 复用，消费者也只是对
  `completed` 做 exchange，不存在 use-after-free；
- producer 注销后，槽位里尚未被消费的 chunk 仍然留在 `completed` 中，后续 drain 还能取到；
- 新 producer 复用槽位时，会在旧链之上继续 CAS 挂新 chunk。消费者反转链表后，
  旧 producer 的 chunk 仍然排在新 chunk 前面，顺序保持。

队列在构造时创建 `max_producer_count` 个槽位，每个槽位独立 cache line。

### 4.3 `chunk_pool`

chunk 池使用 `detail::bounded_mpmc_queue<chunk*>`，算法来自 Dmitry Vyukov 的有界 MPMC 队列：

- 底层是一个按 2 的幂取整的 slot 数组；
- 每个 slot 自带 sequence number，用 CAS 推进 `enqueue_position` / `dequeue_position`；
- 通过 sequence number 判断“当前位置可写/可读”，不需要外部内存回收，也没有 Treiber 栈的 ABA 问题；
- 生产者从池中 dequeue 一个空闲 chunk；
- 消费者 drain 完 chunk 后 enqueue 回池；
- 全程无锁、无阻塞、无分配。

注意：`sharded_queue` 构造参数里的 `chunk_pool_capacity` 是**实际 chunk 数量**；
池内部 slot 数会向上取整到 2 的幂，这只影响内部环形数组的大小，不会让队列拥有
更多 chunk，也不会改变容量语义。

### 4.4 `producer`

```cpp
class producer {
  std::shared_ptr<queue_type> p_queue_; // 持有队列，保证队列存活
  entry_type* entry_;                   // 指向队列持有的槽位
  chunk_type* current_;                 // 正在填充的 chunk；可能为空
};
```

- producer 不是线程安全的，同一时刻只能由一个线程使用；
- 一个 producer 对应一个槽位，producer 析构时槽位被释放；
- `p_queue_` 保证 producer 存活期间队列不会被销毁；
- `current_` 是普通指针，只有拥有该 producer 的线程访问。

### 4.5 计数器与只读属性

| 成员 | 含义 |
| --- | --- |
| `dropped_count_` | 被丢弃的 **item 数量** |
| `pending_chunk_count_` | 已发布但尚未 drain 的 **chunk 数量** |
| `chunk_pool_capacity_` | 构造时指定的 chunk 数量 |
| `max_producer_count_` | 写者槽位数量 |

两个计数器都是 `std::atomic`，使用 relaxed 内存序：

- 单次读取是“快照”，生产者仍在运行时可能不是精确值；
- 在所有生产者停止、消费者完成 drain 之后读取是准确的；
- 它们用于监控和调优，不参与队列正确性判断。

## 5. 并发原理

### 5.1 发布链为什么必须用 CAS

消费者通过 `exchange` 一次性取走整条 completed 链。如果生产者用普通的
“load + store” 发布：

```cpp
// 错误写法
finished->next = entry_->completed.load(relaxed);
entry_->completed.store(finished, release);
```

存在下面的交错：

1. 生产者读到旧链头 `A`；
2. 消费者 `exchange` 取走 `A` 及其后续 chunk；
3. 生产者执行 `store`，把 `finished` 写入槽位，且 `finished->next` 仍指向已被取走的 `A`；
4. 下一次消费者会再次遍历 `A`，而 `A` 可能已经被回收并复用，造成重复消费或数据损坏。

正确做法是在循环中 CAS：

```cpp
chunk* head = entry_->completed.load(relaxed);
for (;;) {
  finished->header.next = head;
  if (entry_->completed.compare_exchange_weak(
          head, finished, std::memory_order_release, std::memory_order_relaxed)) {
    break;
  }
}
```

如果 CAS 期间链头被消费者取走，CAS 会失败，`head` 被更新为最新值（通常是
`nullptr`），生产者重新设置 `next` 后重试。这样不会把已取走的链重新挂回去。

### 5.2 发布与 pending 计数的顺序

`publish_current()` 先执行 `pending_chunk_count_++`，再做 CAS 发布：

```text
pending_chunks++  ->  CAS 发布 chunk
```

原因：消费者回收 chunk 时会 `pending_chunks--`。必须保证“chunk 已可见”时，
对应的 `++` 已经发生，否则计数可能下溢。由于 `pending++` 在 release CAS 之前，
消费者 acquire exchange 之后一定能看到这次递增。

CAS 重试不会重复计数：`pending++` 在循环外只执行一次。

### 5.3 消费者为什么不会访问到已销毁的 producer

如果 completed 链放在 producer 对象里，消费者扫描生产者列表时就可能出现：

- 消费者拿到 producer 原始指针；
- 另一个线程析构了 producer；
- 消费者解引用悬空指针。

本实现把 completed 链放在**队列持有的 writer_entry** 中：

- 槽位数组在队列构造时创建，直到队列销毁才释放；
- 消费者只访问 `writers_[i].completed`，不需要解引用 producer；
- producer 析构时只需要 flush、撤销 owner、归还槽位；
- 槽位复用时旧 chain 仍可被消费者安全取走。

因此，producer 的创建、析构、槽位复用都可以和消费者扫描并发进行。

### 5.4 内存序

| 操作 | 内存序 | 作用 |
| --- | --- | --- |
| producer 发布 CAS | `release` | 保证 chunk 内 item 写入先于发布可见 |
| consumer exchange | `acquire` | 保证看到发布前的所有 item 写入 |
| 池 dequeue/enqueue | acquire/release sequence | 保证 chunk 内容和指针安全传递 |
| `dropped_count` / `pending_chunks` | `relaxed` | 仅监控用途 |
| producer 注册/注销 | `std::mutex` | 串行化槽位分配和 owner 更新 |

item 本身在 chunk 内是普通内存写入，不需要每个 item 一个原子操作；发布/消费
的 release/acquire 配对把整块 chunk 的数据一起传递。

### 5.5 为什么生产者路径仍然是无锁的

- `push` / `emplace` 不获取 mutex；
- 发布使用无锁 CAS；
- 取空闲 chunk 使用无锁队列；
- 池耗尽时直接丢弃，不等待。

唯一使用 mutex 的地方是：

- `acquire_producer()`：分配槽位；
- `producer` 析构：注销槽位；
- `multithread_sharded_queue` 的 TLS 缓存首次获取 producer 时也会间接触发。

这些都是低频操作，不在每次 push 的热路径上。

## 6. 各函数逻辑

### 6.1 快速参考

| 函数 | 作用 | 关键逻辑 |
| --- | --- | --- |
| `sharded_queue(chunks, producers)` | 构造队列 | 预分配 chunk 和槽位，填充空闲池 |
| `drain()` | 取出并返回所有已发布 item | 分配 vector，调用 `drain_into` |
| `drain_into(out)` | 追加取出所有已发布 item | 扫描槽位、反转链、移动 item、回收 chunk |
| `dropped_count()` | 查询丢弃 item 数 | relaxed 读取 |
| `pending_chunks()` | 查询未消费 chunk 数 | relaxed 读取 |
| `request_flush_all()` | 请求所有 producer 尽快 flush | 递增 generation，不阻塞、不唤醒线程 |
| `chunk_pool_capacity()` | 查询 chunk 数 | 返回构造参数归一化后的值 |
| `max_producer_count()` | 查询槽位数 | 返回构造参数归一化后的值 |
| `acquire_producer()` | 获取写者句柄 | mutex 分配槽位；槽位满返回 `nullptr` |
| `producer::push(item)` | 写入一个 item | 入口先检查 flush 请求；必要时发布旧 chunk、取新 chunk；失败则丢弃 |
| `producer::emplace(args...)` | 原地构造一个 item | 同 push，构造发生在 chunk 内 |
| `producer::flush()` | 发布当前 chunk | 非空则发布，空则归还池；同时确认入口时已可见的 flush 请求 |
| `producer::flush_if_requested()` | 检查并响应 flush 请求 | 有新请求则发布当前 chunk，返回是否观察到新请求 |
| `producer::take_completed()` | 取走该写者的 chunk 链 | 低层接口，返回 LIFO 链 |
| `producer::reuse_chunk(chunk*)` | 归还手动取走的 chunk | 清空剩余 item 后回收 |
| `producer::~producer()` | 析构 | 自动 flush + 注销槽位 |
| `multithread_sharded_queue(queue)` | 包装队列 | 保存 shared queue，生成唯一 id |
| `multithread_sharded_queue::push/emplace` | 通过 TLS producer 写入 | 首次使用时自动 `acquire_producer` |
| `multithread_sharded_queue::flush` | flush 当前线程的 producer | 没有 producer 时无操作 |
| `multithread_sharded_queue::request_flush_all` | 请求所有 producer flush | 转发到底层队列 |
| `multithread_sharded_queue::flush_if_requested` | 检查当前线程 producer | 不创建 producer；没有 producer 时返回 false |
| `multithread_sharded_queue::drain/drain_into` | 转发给底层队列 | 单消费者调用 |
| `multithread_sharded_queue::~...` | 清理当前线程缓存 | 其他线程的 TLS producer 到线程退出才释放 |

下面展开关键函数的内部逻辑。

### 6.2 `sharded_queue` 构造函数

```cpp
explicit sharded_queue(std::size_t chunk_pool_capacity = 64,
                       std::size_t max_producer_count = 64);
```

执行步骤：

1. 将构造参数中的 0 归一化为 1；
2. 分配 `chunk_pool_capacity_` 个 `chunk` 对象（`std::make_unique<chunk[]>`）；
3. 构造内部 `chunk_pool_`，其 slot 数向上取整到 2 的幂；
4. 分配 `max_producer_count_` 个 `writer_entry`；
5. 把所有 chunk 指针 enqueue 到空闲池中。

此时所有 chunk 都可用，所有槽位 owner 为空。构造之后 `push` 不会再分配 chunk。

### 6.3 `drain()` 与 `drain_into()`

`drain()`：

```cpp
std::vector<T> drain();
```

1. 读取 `pending_chunks()` 作为容量提示；
2. `items.reserve(pending * ChunkSize)`；
3. 调用 `drain_into(items)`；
4. 返回 vector。

`pending` 只是构造时的快照，并发发布时可能偏大或偏小，`reserve` 只影响性能，
不影响正确性。

`drain_into(out)` 的核心步骤：

1. **收集**：`collect_published_chunks()` 遍历所有 `writer_entry`；
2. 对每个槽位执行 `completed.exchange(nullptr, acquire)`，取走该写者的整条链；
3. 每条链是 LIFO（新 chunk 在头部），先原地反转成发布顺序；
4. 把各写者的链拼接到一条总链上；
5. 依次处理每个 chunk：
   - 把 chunk 内 item 逐个移动构造到 `out`；
   - 销毁 chunk 内原对象；
   - 把已清空的 chunk 回收：`pending_chunks--`，再 enqueue 回空闲池；
6. 返回后，本次扫描到的 chunk 都回到池中，生产者可以再次取用。

重要语义：

- **只处理已发布 chunk**。producer 正在填充的 `current_` 不会被 drain；
- 即使一个 chunk 已经写满，只要它仍然是 `current_`，也不会自动发布；
  需要下一次 push（发现满了）或 `flush()` / producer 析构；
- `drain_into` **追加**到 `out`，不会先 `clear()`；
- 消费者必须只有一个线程调用 `drain` / `drain_into`。

异常行为：如果 item 的移动构造或 vector 扩容抛出异常，当前 chunk 的剩余 item
会被销毁，剩余 chunk 会被清空并回收，异常继续向上抛出；已经追加到 `out` 的
item 会保留。队列本身仍然保持一致，可以继续使用。

### 6.4 `acquire_producer()`

```cpp
[[nodiscard]] std::shared_ptr<producer> acquire_producer();
```

执行步骤：

1. 加 `registry_mutex_`；
2. 如果 `producer_count_ >= max_producer_count_`，返回 `nullptr`；
3. 线性扫描 `writers_`，找第一个 `owner == nullptr` 的槽位；
4. 构造 `producer`：
   - 通过 `shared_from_this()` 保存队列的 `shared_ptr`；
   - 记录槽位指针；
   - `current_` 初始为 `nullptr`，第一次 push 时才取 chunk；
   - `observed_flush_epoch_` 初始化为队列当前 flush generation；
5. 设置槽位 owner，递增 `producer_count_`，返回 `shared_ptr<producer>`。

注意事项：

- 队列必须由 `std::shared_ptr` 持有，否则 `shared_from_this()` 会抛出
  `std::bad_weak_ptr`；
- 返回 `nullptr` 表示所有槽位都被占用；
- 直接使用 producer 时，用完应尽快释放，以归还槽位；
- `multithread_sharded_queue` 内部会调用此函数，并把 producer 缓存在 TLS 中。

### 6.5 计数与容量查询

```cpp
std::uint64_t dropped_count() const noexcept; // 被丢弃的 item 数
std::size_t pending_chunks() const noexcept;  // 已发布未消费的 chunk 数
std::size_t chunk_pool_capacity() const noexcept;
std::size_t max_producer_count() const noexcept;
```

- `dropped_count` 统计 **item**，不是 chunk；
- `pending_chunks` 统计 **chunk**，一个 chunk 最多包含 `ChunkSize` 个 item；
- 两者都是 relaxed 快照；生产者停止后读取才是最终值；
- 通过 `multithread_sharded_queue` 写入时，“没有可用 producer 槽位”导致的丢弃
  也计入 `dropped_count`。

### 6.6 `producer::push()`

```cpp
template <typename U>
  requires(std::is_same_v<std::remove_cvref_t<U>, T>)
void push(U&& item);
```

伪代码：

```cpp
flush_if_requested(); // 入口检查 flush 请求；有请求则先发布旧 chunk

if (!ensure_writable_chunk()) {
  record_drop(); // dropped_count++
  return;
}
current_->try_push(std::forward<U>(item)); // placement new 到 current_
```

`ensure_writable_chunk()` 的逻辑：

```text
current_ != nullptr 且未满
    -> 直接使用

current_ == nullptr
    -> 从 chunk_pool 取一个 chunk
       -> 取不到则返回 false（调用方丢弃 item）

current_ 已满
    -> rotate():
         publish_current()   // 发布满 chunk
         try_acquire_chunk() // 尝试取新 chunk
       -> current_ 可能变成 nullptr
```

典型流程：

- 第一次 push：从池中取 chunk，写入；
- 存在未处理的 flush 请求：先发布当前 chunk，再写入新 item；
- chunk 写满后继续 push：先发布满 chunk，再取新 chunk；
- 池中暂时没有空闲 chunk：item 被丢弃，`dropped_count++`，producer 仍然可用；
- 消费者回收 chunk 后，后续 push 可以重新从池中取到 chunk。

`push` 不分配内存、不加锁、不阻塞；如果 `T` 的构造/移动抛出异常，异常会传播，
当前 chunk 保持原状。

### 6.7 `producer::emplace()`

```cpp
template <typename... Args>
void emplace(Args&&... args);
```

与 `push` 的唯一区别是 item 直接在 chunk 的存储槽内构造，避免临时对象：

```cpp
current_->try_emplace(std::forward<Args>(args)...);
```

同样会在入口检查 flush 请求，并在需要时发布/获取 chunk，失败时丢弃并计数。

### 6.8 `producer::flush()`

```cpp
void flush();
```

逻辑：

1. 读取当前 flush request generation；
2. `current_ == nullptr`：没有数据需要发布，但仍记录该 generation；
3. 当前 chunk 为空：不发布任何数据，把空 chunk 直接归还池；
4. 当前 chunk 非空：调用 `publish_current()` 发布，`current_` 置空；
5. 更新 `observed_flush_epoch_`。

因此显式 `flush()` 会确认它在入口时已经能看到的 `request_flush_all()` 请求；
flush 期间新到的请求会留到下一次检查。调用后 producer 仍然有效；下一次 push
会重新从池中取 chunk。消费者在下一次 `drain` 时能看到刚发布的数据。

`flush` 必须由拥有该 producer 的线程调用；`producer` 不是线程安全的，不能让其他
线程代为 flush。

`flush` 不会阻塞，也不会等待消费者消费。

### 6.9 `producer::rotate()`（私有）

只有在 `current_` 已满时由 `ensure_writable_chunk()` 调用：

1. 如果 `current_` 为空，直接返回；
2. `publish_current()` 发布当前 chunk；
3. 尝试从池中取一个新的空 chunk 赋给 `current_`；
4. 如果池为空，`current_` 变为 `nullptr`，调用方将丢弃本次 item。

### 6.10 `producer::publish_current()`（私有）

负责把一个 chunk 发布到 `writer_entry.completed`：

1. 取出 `current_` 并置空；
2. 如果 chunk 为空，归还池，不增加 `pending_chunks`；
3. `pending_chunk_count_++`；
4. 读取 `entry_->completed` 当前链头；
5. CAS 循环：
   - `finished->next = head`；
   - 尝试 CAS 把 `finished` 设为新链头；
   - 失败时 `head` 被更新，重试；
6. 成功后 chunk 对消费者可见。

使用 release CAS 确保 chunk 内所有 item 写入在发布前完成。

### 6.11 `producer::take_completed()`

```cpp
chunk_type* take_completed() noexcept;
```

低层接口：直接 exchange 掉当前写者的整条 completed 链。

- 返回的是 **LIFO 链**：最新发布的 chunk 在头部；
- 调用者取得 chunk 的所有权，必须自己遍历、移出 item，并对每个 chunk 调用
  `reuse_chunk()` 归还；
- 如果只想要正确的写入顺序，使用 `sharded_queue::drain()` / `drain_into()`，
  它们会自动反转链并回收 chunk；
- 不要和 `drain()` / `drain_into()` 并发调用；通常只有消费者会使用它。

### 6.12 `producer::reuse_chunk()`

```cpp
void reuse_chunk(chunk_type* p_chunk);
```

用于归还通过 `take_completed()` 手动取走的 chunk：

1. 如果指针为空，直接返回；
2. `clear()`：销毁 chunk 中可能剩余的 item；
3. `recycle_chunk()`：
   - `pending_chunks--`；
   - enqueue 回空闲池。

如果手动处理时只移出了一部分 item，剩余 item 会被 `clear()` 销毁，不会泄漏，也
不会自动放回队列。

### 6.13 `producer::~producer()`

析构顺序：

1. `flush()`：发布当前未发布的 chunk（如果为空则归还池）；
2. `unregister_producer(this)`：加锁，清空槽位 owner，`producer_count_` 减一；
3. 释放 `p_queue_`。

由于 completed 链位于队列的槽位中，producer 析构后，已经发布的 chunk 仍然
留在槽位里，消费者后续 drain 还能取到。这正是 `writer_entry` 由队列持有的收益。

注意：producer 析构不会等待消费者消费，也不会清空该槽位的 completed 链。

### 6.14 `multithread_sharded_queue` 构造与析构

```cpp
explicit multithread_sharded_queue(std::shared_ptr<queue_type> queue);
~multithread_sharded_queue();
```

构造：

- 保存底层队列的 `shared_ptr`；
- 生成一个该模板实例内唯一的 wrapper id（原子计数器）；
- 如果队列为空，抛出 `std::invalid_argument`。

析构：

- 只清理**当前线程** TLS 缓存中属于本 wrapper 的 producer；
- 其他线程缓存中的 producer 不会被主动销毁，它们持有底层队列并在线程退出时
  才被释放。

因此，必须确保 writer 线程停止后再销毁 wrapper；在 writer 仍使用 wrapper 时
销毁它属于未定义行为。

### 6.15 `multithread_sharded_queue::push/emplace/flush/request_flush_all`

`push`：

1. `local_producer()` 查找当前线程缓存的 producer；
2. 没有则调用底层 `acquire_producer()` 获取并缓存；
3. 获取失败（槽位耗尽）则 `dropped_count++`，item 丢弃；
4. 调用 producer 的 `push`。

`emplace` 与 `push` 相同，只是把构造参数转发给 producer。

`flush`：

- 只 flush 当前线程已有的 producer；
- 不会为了 flush 而创建 producer；
- 没有 producer 时无操作；
- 会确认当前线程 producer 在 flush 入口时已可见的 flush 请求。

`request_flush_all`：

- 直接转发到底层队列，只递增全局 generation；
- 不会遍历其他线程的 TLS 缓存，也不会唤醒其他线程。

`flush_if_requested`：

- 只在当前线程缓存中查找 producer；
- 找到则调用其 `flush_if_requested()`；
- 没有 producer 时返回 false，不创建新 producer。

底层队列的 `drain` / `drain_into` 直接转发，仍然要求单消费者。

### 6.16 `multithread_sharded_queue` 的私有辅助函数

| 函数 | 逻辑 |
| --- | --- |
| `next_id()` | 每个模板实例一份静态原子计数器，给 wrapper 分配唯一 id |
| `thread_cache()` | 每个模板实例、每个线程一份 `vector<cache_entry>` |
| `find_local_producer()` | 在线性表里查找当前 wrapper 的 producer，不创建 |
| `local_producer()` | 先查缓存，没有则 acquire 并插入缓存 |
| `clear_thread_cache()` | 析构时删除当前线程中对应 id 的缓存项 |

TLS 缓存按 wrapper 实例 id 区分，避免不同队列/不同 wrapper 共用同一个 producer。

### 6.17 `detail::bounded_mpmc_queue`

`try_enqueue(value)`：

1. 读取 enqueue 位置；
2. 检查对应 slot 的 sequence 是否等于当前位置；
3. 是则 CAS 推进 enqueue 位置，写入 value，再以 release 写入 `position + 1`；
4. 如果 sequence 落后，说明队列满，返回 false；
5. 如果 sequence 领先，说明其他线程已推进位置，重新读取。

`try_dequeue(out)`：

1. 读取 dequeue 位置；
2. 检查 slot sequence 是否等于 `position + 1`；
3. 是则 CAS 推进 dequeue 位置，读出 value，再以 release 写入
   `position + mask + 1`，把 slot 标记为可再次 enqueue；
4. 如果 sequence 落后，说明队列空，返回 false。

该结构只被 chunk 池使用，对上层不可见。

### 6.18 `request_flush_all()` 与 `flush_if_requested()`

```cpp
// sharded_queue
void request_flush_all() noexcept;

// sharded_queue::producer
bool flush_if_requested() noexcept;

// multithread_sharded_queue
void request_flush_all() noexcept;
bool flush_if_requested() noexcept;
```

队列内新增一个单调递增的原子 generation：

```cpp
alignas(64) std::atomic<std::uint64_t> flush_request_epoch_{ 0 };
```

`request_flush_all()` 只做 `fetch_add(1, release)`：

- 不发布数据，不修改 chunk，不唤醒线程；
- 多次请求会合并，producer 一次检查只响应最新 generation；
- 任意线程都可以调用，包括 producer 自己。

每个 producer 保存自己已经处理到的 generation：

```cpp
std::uint64_t observed_flush_epoch_;
```

producer 构造时把它初始化为队列当前 generation，因此请求发生在 producer 创建之前
时，新 producer 不承担旧请求的 flush 义务。

`producer::flush_if_requested()`：

1. acquire 读取队列 generation；
2. 如果等于 `observed_flush_epoch_`，返回 false；
3. 如果 `current_` 非空，调用 `publish_current()` 发布；
4. 如果 `current_` 为空，保留这个空 chunk，不产生池往返；
5. 更新 `observed_flush_epoch_`；
6. 返回 true（表示观察到了一次新请求，即使没有数据）。

`push` / `emplace` 在入口自动调用 `flush_if_requested()`：

- 请求已可见时，先发布请求之前的当前 chunk；
- 然后本次 push/emplace 才写入新 item，**新 item 不包含在这次请求触发的 flush 中**；
- 如果 flush 后池已经耗尽，本次 item 仍按既有规则丢弃并计数。

显式 `flush()` 在入口读取并记录 generation，因此可以确认入口时已经可见的请求；
flush 期间新到的请求留给下一次检查。`multithread_sharded_queue::request_flush_all()`
只转发到队列；`multithread_sharded_queue::flush_if_requested()` 只处理当前线程的
TLS producer，不创建新 producer，没有 producer 时返回 false。

内存序说明：generation 本身不携带数据，数据可见性仍由 chunk 的 release CAS /
acquire exchange 保证；请求使用 release/acquire 让跨线程信号更直观。

性能：每次 push/emplace 增加一次 acquire 原子读取和一个可预测分支。基准测试中
`BM_FlushRequestPoll` 单独测量该检查，`BM_RequestFlushRoundTrip` 测量请求驱动的
发布/消费往返；纯 push 的高吞吐场景会看到相应开销，应结合业务对延迟的要求使用。

## 7. 语义细节

### 7.1 可见性：什么时候能 drain 到数据

数据对消费者可见的条件：

1. 它所属的 chunk 已经发布；
2. 消费者调用 `drain` / `drain_into`。

chunk 发布的时机：

- 生产者继续 push，发现 `current_` 已满，触发 `rotate()` 发布；
- 生产者显式调用 `flush()`；
- producer 在 push/emplace 入口自动观察到 `request_flush_all()` 请求；
- 空闲 producer 主动调用 `flush_if_requested()` 并观察到请求；
- producer 析构自动 flush。

注意：`request_flush_all()` 本身只设置请求，不代表数据已经发布。producer 没有运行
检查逻辑时，数据仍停留在它的 `current_` 中。

因此，**刚写满但还没有下一次 push 的 chunk 不会自动可见**。例如
`ChunkSize == 4` 时连续 push 4 个 item，然后直接 drain，结果为空；必须再 push
第 5 个（触发发布）或调用 `flush()`。

这不是 bug，而是批量发布策略的一部分：把发布成本集中到 chunk 边界，减少原子
操作次数。

### 7.2 顺序

- **单个 producer 内**：drain 返回的 item 严格按 push 顺序；
- **chunk 之间**：同一 producer 的 chunk 按发布顺序；
- **不同 producer 之间**：没有全局顺序保证，drain 的槽位扫描顺序决定了一个
  确定但非业务语义的顺序；
- 同一 producer 的 item 不会乱序，因为发布链反转后按发布顺序处理。

如果需要全局顺序，需要业务层额外加序号或合并逻辑。

### 7.3 丢弃与背压

丢弃发生的情况：

1. 生产者需要新 chunk，但空闲池为空；
2. 通过 `multithread_sharded_queue` 写入时，没有可用 producer 槽位。

行为：

- 当前 item 不被写入；
- `dropped_count` 加一；
- 调用方不会阻塞；
- 队列状态保持有效，后续 push 仍可成功。

丢弃的是 item，不是整个 chunk。chunk 池大小决定了系统能缓冲多少未消费数据；
消费者长期不 drain 时，池最终会被已发布的 chunk 和生产者手里的 current chunk
占满，此后需要新 chunk 才能继续写入的 push 都会被丢弃。

高频调用 `request_flush_all()` 会让 producer 频繁提前发布半满 chunk，并立刻需要
新的 chunk。请求越频繁，池的周转压力越大，在池容量紧张时可能增加丢弃；应把请求
控制在消费端真正需要低延迟的时机（例如每帧一次或显式同步点）。

### 7.4 计数器语义

- `pending_chunks()`：已发布、尚未被 drain 回收的 chunk 数；
- 一个 chunk 即使已经部分/全部被处理，只要还没 recycle，仍计入 pending；
- 通过 `take_completed()` 手动取走后，pending 不会立即减少，直到
  `reuse_chunk()` 被调用；
- `dropped_count()`：累计丢弃 item 数，不是当前队列中的 item 数；
- 两者都是 relaxed 快照，监控时不要依赖精确的瞬时一致性。

### 7.5 生命周期

- 队列必须由 `std::shared_ptr` 持有；
- 每个 producer 持有队列的 `shared_ptr`，因此 producer 存活时队列不会被销毁；
- 队列销毁时，所有 chunk 的析构函数会销毁其中尚未取走的 item；
- producer 析构会把 `current_` 中的数据 flush 到槽位，但不能保证消费者之后一定
  会 drain；如果消费者已经退出，这些 item 会在队列销毁时被销毁；
- 正确的关闭顺序见 8.5 节。

### 7.6 对 `T` 的要求

- 必须可析构；
- `drain` 使用 `std::move` + `vector::emplace_back`，因此需要可移动构造，
  或至少有可接受右值的拷贝构造；
- 不要求默认构造；
- 支持 move-only 类型；
- 建议析构函数不抛异常；chunk 析构是 `noexcept` 路径；
- 如果 T 的移动构造可能抛异常，队列会保持一致，但 `drain_into` 可能已经向
  输出 vector 追加了部分 item。

### 7.7 异常行为摘要

| 场景 | 行为 |
| --- | --- |
| `push`/`emplace` 中 T 构造抛异常 | 当前 chunk 不变，异常传播，item 不进入队列 |
| `drain_into` 中移动/分配抛异常 | 销毁剩余 item、回收剩余 chunk，异常传播；已追加到 out 的 item 保留 |
| `acquire_producer` 时队列不是 shared_ptr 持有 | 抛出 `std::bad_weak_ptr` |
| `acquire_producer` 槽位耗尽 | 返回 `nullptr` |
| `multithread_sharded_queue` 构造传空队列 | 抛出 `std::invalid_argument` |

## 8. 用法说明

### 8.1 直接使用 producer

```cpp
#include <memory>
#include <vector>

#include "cyberverse/algorithm/container/sharded_queue.hpp"

using event_queue = cyberverse::algorithm::sharded_queue<event, 256>;

auto queue = std::make_shared<event_queue>(/*chunks=*/64, /*producers=*/4);

const std::shared_ptr<event_queue::producer> producer = queue->acquire_producer();
if (!producer) {
  // 所有写者槽位都被占用
}

producer->emplace(/* event 构造参数 */);
producer->push(event{/* ... */});
producer->flush(); // 发布当前 chunk

std::vector<event> batch;
queue->drain_into(batch); // 追加到 batch
```

生产者线程可以持有自己的 `producer`，消费者线程持有队列并周期 drain。注意
`producer` 不能跨线程共享。

### 8.2 使用多线程包装器

```cpp
using event_queue = cyberverse::algorithm::sharded_queue<event, 256>;
auto queue = std::make_shared<event_queue>(/*chunks=*/64, /*producers=*/8);

cyberverse::algorithm::multithread_sharded_queue<event, 256> shared_queue(queue);

std::thread worker([&shared_queue] {
  for (int i = 0; i < 1000; ++i) {
    shared_queue.emplace(/* ... */);
  }
  shared_queue.flush(); // 线程退出时也会自动 flush
});

worker.join();

std::vector<event> batch = shared_queue.drain();
```

包装器首次 push 时自动创建 producer；线程退出时 TLS 缓存销毁，producer 自动
flush 并注销。`max_producer_count` 至少要不小于同时存活的写线程数。

### 8.3 消费者循环

```cpp
std::atomic<bool> stop{false};
std::vector<event> batch;

std::thread consumer([&] {
  while (!stop.load(std::memory_order_acquire)) {
    batch.clear();
    queue->drain_into(batch);

    for (const event& item : batch) {
      handle(item);
    }

    // 实际项目中不要纯忙等，可以用条件变量、事件通知或帧信号。
    std::this_thread::yield();
  }

  // 生产者停止后再做最后一次 drain。
  batch.clear();
  queue->drain_into(batch);
  for (const event& item : batch) {
    handle(item);
  }
});
```

队列本身不提供“有数据可读”的通知，消费者需要外部驱动（帧循环、事件循环、
条件变量等）。

### 8.4 帧循环集成

典型做法是“每帧末尾 flush，下一帧开头 drain”。

如果主线程是唯一的生产者，可以直接在主线程 flush：

```cpp
void end_frame(const std::shared_ptr<event_queue::producer>& producer) {
  producer->flush(); // 把本帧写入的当前 chunk 发布
}

void begin_frame() {
  batch.clear();
  queue->drain_into(batch);
  process(batch);
}
```

如果有多个 worker 生产者，**每个 producer 必须由拥有它的线程 flush**，不能让主线程
遍历并 flush 其他线程的 producer：

```cpp
// 每个 worker 在自己的线程内调用，p 是该线程独占的 producer。
void writer_frame_end(event_queue::producer& p) {
  p.flush();
}

// 或者使用包装器：调用只影响当前线程的 TLS producer。
void writer_frame_end(cyberverse::algorithm::multithread_sharded_queue<event, 256>& shared_queue) {
  shared_queue.flush();
}
```

- flush 只是发布，不等待消费者；
- 生产者可以继续写新 chunk；
- 消费者每帧 drain 一次，避免池长期占用；
- 如果一帧内产生的 item 很多，可多次调用 `drain_into` 或增大 chunk 池。

### 8.5 关闭顺序

推荐顺序（消费者是独立线程时）：

1. 停止所有生产者继续提交；
2. join 所有生产者线程；使用 `multithread_sharded_queue` 或线程内局部 producer 时，
   线程退出会自动 flush。若 producer 由其他线程持有，需要在该线程结束前显式
   `flush()` 或释放它；
3. 通知消费者停止；消费者线程在退出循环前执行最后一次 `drain_into`；
4. join 消费者线程；
5. 释放队列。

示例：

```cpp
// 1. 停止写入并等待所有 producer 析构 / flush。
stop_writers.store(true);
for (auto& thread : writer_threads) {
  thread.join();
}

// 2. 通知消费者退出。消费者循环在退出前会做最后一次 drain。
stop_consumer.store(true);
consumer.join();

// 3. 消费者已经停止，此时当前线程成为唯一的消费者，可以安全地再 drain 一次。
std::vector<event> remaining;
queue->drain_into(remaining);
process(remaining);

queue.reset(); // 若还有未取走的数据，析构时销毁
```

如果消费者就是主线程（没有独立 consumer 线程），则在 join 所有 producer 之后
直接由主线程 drain：

```cpp
stop_writers.store(true);
for (auto& thread : writer_threads) {
  thread.join();
}
std::vector<event> remaining;
queue->drain_into(remaining);
process(remaining);
```

关键点：**不能在一个消费者线程还在 drain 时，让另一个线程也调用 drain。**
必须先停止旧消费者，再让新线程接管；或者始终由同一个线程消费。

注意：`queue.reset()` 不会把剩余 item 交给消费者，只会销毁它们。要保留数据，
必须在释放队列前 drain。

### 8.6 参数选择

#### `ChunkSize`

| 选择 | 影响 |
| --- | --- |
| 较大（如 256、512） | 发布 CAS、池操作更少，吞吐更高；item 可见延迟更大，内存占用更高 |
| 较小（如 16、32） | 数据更早可见，适合低延迟；原子操作和 chunk 元数据开销更高 |

建议从 64～256 开始，根据 item 大小、写入速率和可接受延迟调整。

#### `chunk_pool_capacity`

大致决定可缓冲的 item 上限：

```text
最大缓冲 item 数 <= chunk_pool_capacity * ChunkSize
```

实际可用量可能略小，因为 producer 会持有当前 chunk，已发布未消费的 chunk 也
占用池。消费者处理慢、突发流量大时增大该值；如果 `dropped_count()` 持续增长，
说明池不够或消费者 drain 不够频繁。

#### `max_producer_count`

表示**同时存在的 producer 数量上限**，不是历史线程总数：

- 直接使用 producer：producer 存活期间占用槽位；
- 包装器：每个调用过 push 的线程占用一个 TLS producer，直到线程退出；
- 超过上限时，直接 acquire 返回 `nullptr`，包装器 push 丢弃并计数。

建议设置为“最大并发写线程数 + 余量”，不要盲目设得过大，因为每个槽位都会占用
独立 cache line。

### 8.7 监控

```cpp
const std::uint64_t dropped = queue->dropped_count();
const std::size_t pending_chunks = queue->pending_chunks();

if (dropped > 0) {
  // 池太小、消费者太慢，或 producer 槽位不足
}

if (pending_chunks > 0) {
  // 有已发布但尚未消费的数据；单位是 chunk，不是 item
}
```

生产环境可以把这两个值接到性能计数器或 HUD 上，用于观察消费是否跟得上。

### 8.8 如何让 flush 请求尽快生效

`request_flush_all()` 是协作式信号，不是中断，也不会唤醒线程。数据从“已 flush”
到“可 drain”仍需要 producer 线程执行检查。

按 producer 的状态选择方式：

| producer 状态 | 最快响应方式 |
| --- | --- |
| 正在持续 push/emplace | 无需额外代码；入口自动检查请求 |
| 周期性运行（帧循环、批处理） | 每个周期调用 `flush_if_requested()` |
| 阻塞在条件变量/事件上 | 应用层先唤醒 writer，再让它调用 `flush_if_requested()` |
| 已停止/不再运行 | 无法响应；队列只会 flush 生产者析构时已经发布的内容 |

消费者侧的低延迟模式：

```cpp
queue->request_flush_all();

// 让 producer 跑过检查点。可以用帧屏障、线程屏障、
// 或者等待 writer 返回本轮处理完成的信号。
wait_for_writers_to_reach_checkpoint();

std::vector<event> batch;
queue->drain_into(batch);
```

writer 侧的空闲轮询：

```cpp
while (running) {
  // 有请求时发布当前 chunk；返回 true 表示观察到了一次新请求。
  producer.flush_if_requested();

  // 没有工作的时候，writer 可以阻塞在自己的条件变量上；
  // request_flush_all() 不会唤醒它，必须由应用层负责通知。
  wait_for_work();
}
```

`multithread_sharded_queue` 的对应写法：

```cpp
// 消费端/协调线程：
shared_queue.request_flush_all();

// 每个 writer 线程在自己的循环里：
shared_queue.flush_if_requested(); // 只处理当前线程的 TLS producer
```

注意：

- 不要在 `request_flush_all()` 之后立刻假定 drain 能拿到数据；producer 可能还没
  轮到检查。
- 不要从其他线程直接调用某个 producer 的 `flush()` 或 `flush_if_requested()`；
  producer 不是线程安全的。
- 如果业务需要“等待所有 producer 确认”，需要额外的 generation ack/超时协议；
  本实现不提供阻塞等待，避免 producer 空闲时死锁。

## 9. 常见问题

### Q1：push 之后马上 drain 为什么是空的？

因为 chunk 还没发布。需要：

- 写满后继续 push（触发发布）；
- 或显式 `flush()`；
- 或让 producer 析构。

注意：即使 chunk 已经写满，只要它还是 `current_`，也不会自动可见。

### Q2：`pending_chunks()` 为 0，是不是队列空了？

不一定。它只统计**已发布**的 chunk；producer 手里未发布的 `current_` 不计入。
另外它统计 chunk 数量，不是 item 数量。

### Q3：`dropped_count()` 增加说明什么？

说明有 item 没有进入队列。原因可能是：

- chunk 池耗尽；
- 通过 `multithread_sharded_queue` 写入时，producer 槽位耗尽。

这不是“消费失败”，而是写入端被限流/丢弃。

### Q4：可以多个消费者同时 drain 吗？

不可以。`drain` / `drain_into` 只能由一个消费者线程调用。

如需并行消费，请创建多个队列，每个消费者独占一个队列。

### Q5：可以把同一个 producer 给多个线程用吗？

不可以。producer 不是线程安全的，同一时刻只能由一个线程写入。

### Q6：可以阻塞等待数据吗？

队列本身不提供阻塞接口。可以用外部条件变量/事件循环，在消费者循环里
drain 后等待下一帧或下一个信号。

### Q7：为什么队列必须由 `shared_ptr` 持有？

队列继承 `std::enable_shared_from_this`，producer 通过 `shared_from_this()`
持有队列，以保证 producer 生命周期内队列一直存活。如果队列不是由
`shared_ptr` 持有，`acquire_producer()` 会抛出 `std::bad_weak_ptr`。

### Q8：`take_completed()` 返回的顺序为什么是反的？

completed 链是 LIFO：最新发布的 chunk 在头部。`take_completed()` 直接返回
这条链，因此需要自己反转，或者按“从尾到头”的顺序处理。正常使用
`drain` / `drain_into` 即可，它们会自动反转。

### Q9：`reuse_chunk()` 会保留 chunk 里没取走的 item 吗？

不会。它先 `clear()`，销毁剩余 item，再回收 chunk。只有确定不再需要这些 item
时才调用。

### Q10：支持 move-only 类型吗？

支持。chunk 使用原始存储 + placement new，`drain` 通过 `std::move` 移出 item。
不要求默认构造，也不要求可拷贝。

### Q11：生产者路径真的完全没有锁吗？

`push` / `emplace`、chunk 发布、chunk 池操作都不加锁。只有 producer 的注册和
注销使用 mutex，不在每次写入的热路径上。

### Q12：消费者扫描所有槽位会不会很慢？

`drain` 的固定开销是 `O(max_producer_count)` 次原子 exchange，加上
`O(本次 chunk 数 + item 数)` 的搬运。`max_producer_count` 建议贴近实际并发写者
数量；默认 64 个槽位在大多数场景下开销很小。

### Q13：调用 `request_flush_all()` 后马上 drain 为什么为空？

因为请求只递增 generation，不会直接发布任何 chunk。producer 必须在下一次
`push`/`emplace` 或显式 `flush_if_requested()` 时观察并处理它。如果 producer
空闲或阻塞，消费者需要先通过帧/线程屏障让它运行检查逻辑。

### Q14：可以等待所有 producer 都 flush 完成吗？

本实现不提供同步等待。`flush_if_requested()` 返回的是“该 producer 是否观察到新
请求”，不是全局完成通知；空闲 producer 可能永远不轮询，阻塞等待会死锁。

需要强同步时，应在应用层增加 generation ack + 超时/屏障协议，或让所有 writer
在线程/帧同步点主动调用 `flush_if_requested()`。

### Q15：多次 `request_flush_all()` 会丢失吗？

不会“丢失数据”，但请求会按 generation 合并。producer 一次检查看到最新
generation 时，只会 flush 一次当前 chunk；它不会为每个中间请求分别 flush。

### Q16：`request_flush_all()` 和 `flush()` 是什么关系？

`request_flush_all()` 只是请求；`flush()` 是立即发布当前 chunk 的动作。
显式 `flush()` 会记录入口时已经可见的 generation，因此可以确认那些请求。
`flush_if_requested()` 则是“检查请求并发布当前 chunk”的组合操作。

## 10. 测试与基准

测试文件：[sharded_queue_test.cpp](../test/sharded_queue_test.cpp)

覆盖：

- 发布顺序、跨 chunk 顺序、`drain_into` 追加；
- `flush` 与 producer 析构自动 flush；
- `request_flush_all` 的协作式响应、请求合并、自动检查、显式 flush 确认；
- 请求后新 producer 不受旧 generation 影响；
- 请求触发 flush 与池耗尽丢弃的交互；
- chunk 池耗尽、丢弃计数、回收恢复；
- 非 2 的幂池容量；
- writer 槽位复用与遗留 chunk；
- 多线程并发写、边写边 drain、并发 flush 请求、writer 频繁创建销毁；
- move-only / 非默认构造 / 析构计数；
- 小池高竞争下的“收到数 + 丢弃数 == 提交数”不变量；
- `multithread_sharded_queue` 的 TLS producer、flush 请求转发和槽位耗尽。

运行：

```bash
cmake -S . -B /tmp/cyberverse-algorithm-test
cmake --build /tmp/cyberverse-algorithm-test
ctest --test-dir /tmp/cyberverse-algorithm-test --output-on-failure
```

基准测试文件：[sharded_queue_benchmark.cpp](../benchmark/sharded_queue_benchmark.cpp)

```bash
/tmp/cyberverse-algorithm-test/benchmark/cyberverse_algorithm_benchmarks
```

基准覆盖单生产者 push/drain、纯 push、无请求时的 `flush_if_requested()` 轮询、
request-flush-drain 往返，以及 1/2/4/8 生产者的并发批量吞吐。具体数值依赖机器、
编译配置和系统负载，建议在同一台机器上对比修改前后的结果。
