## Query Scheduling


### Last Class

我们讨论了查询处理模型。

- 矢量化模型最适合OLAP
- 从下到上(push)的方法可能更好

### Scheduling

对于每个查询计划，dbms必须决定where, when, how 执行它。

- 它应该使用多少个任务?
- 它应该使用多少CPU内核?
- 任务应该在哪个cpu核心上执行?
- 任务应该在哪里存储它的输出?

DBMS总是比操作系统知道的更多

> 这节课里面假设数据已经读取到了内存, 忽略磁盘读写


### Scheduling Goals

目标1: 吞吐量

- 最大化完成查询的数量

目标2: 公平

- 确保每一个查询分配资源

目标3: 查询响应性

- 最小化尾部延迟(特别是短查询)

目标4: 低开销

workers应该把大部分时间花在执行任务上，而不是考虑下一个任务是什么。


### Today's Agenda

- Worker Allocation
- Data Placement
- Shceduler Implementations


### Process Model

DBMS的进程模型定义了如何构建系统以支持来自多用户应用程序的并发请求。

worker是负责代表客户端执行任务并返回结果的DBMS组件。

我们假设DBMS是多线程的。


### Worker Allocation

方法1: 每个core一个worker

- 每个内核被分配一个线程，该线程在操作系统中固定在该内核上
- 参见 sched_setaffity

方法2: 每个core有多个worker

- 每个core(或每个socket)使用一个worker池
- 允许CPU核心被充分利用，以防一个worker在一个core阻塞.

### Task Assignment

方法1: Push

- 集中式(centralized)调度程序将任务分配给worker并监控他们的进度
- 当worker通知dispatcher它已经完成时，它会被分配一个新的任务

方法2: Pull

- worker从队列中提取下一个任务，处理它，然后返回获取下一个任务
> 可能还有更复杂的情况, 比如本地队列/全局队列等


### Observation

不管DBMS使用什么worker分配或任务分配策略，worker总是操作(operate on)本地数据(local data)是很重要的。

dbms的调度程序必须知道它的硬件内存布局。
- 统一与非统一内存访问

### Uniform Memory Access

老的方式: 统一内存访问(也叫对称多处理结构), 有一个系统总线(System Bus), 这种体系下没有socket本地内存的概念, 从一个core到不同的内存路径是一样的(cost一样), 系统没有什么需要优化的


### Non-Uniform Memory Access

现代系统重, 使用NUMA方式, 每一个socket都有自己的本地内存. 从本地内存读取数据的速度比从其他地方读取快得多.

Intel(2008): QuickPath Interconnect
Intel(2017): UltraPath Interconnect
AMD(??): HyperTransport
AMD(2017): Infinity Fabric


> 在NUMA下直接malloc分配内存就不太合适, 我们希望DBMS能够精准控制在哪个地方分配内存


### Data Placement (数据放置)

dbms可以为数据库划分内存，并将每个分区分配给一个CPU。

通过控制和跟踪分区的位置，它可以调用operators在最近的CPU核心的工作上执行。
> 我们想要确保读取的数据是本地的

参阅linux的move_pages和numactl


### Memory Allocation

当dbms调用malloc时会发生什么?

- 假设分配器没有可用的内存块

Almost nothing:

- 分配器将扩展进程的数据段
- 但是这个新的虚拟内存不会立即得到物理内存的支持
- 操作系统仅在访问出现页面错误时才分配物理内存

现在，在出现页面错误后，操作系统在NUMA系统中将物理内存分配到哪里?


### Memory Allocation Location

方法1: Interleaving

- 将分配的内存均匀地分布在各个CPU上

方法2: First-Touch

- 访问导致页面错误的内存位置的线程所在的CPU

操作系统可以尝试从观察到的访问模式中将内存移动到另一个NUMA区域。

> 通常在OLAP系统中, 超线程并没有多大的用处


### Partitioning vs. Placement

分区方案用于根据某些策略对数据库进行分割。

- Round-robin
- 属性范围
- 哈希
- 部分复制/完全复制

然后，placement(放置)方案告诉DBMS将这些分区放在哪里。

- Round-robin
- Interleave across cores

### Observation

到目前为止，我们有以下内容:

- 任务分配模型
- 数据placement(放置)策略

但是，我们如何决定如何从逻辑查询计划中创建一组任务呢?

- 这对于OLTP查询来说相对容易 (因为通常只有一个pipeline/task)
- 更难的OLAP查询… (我们必须考虑这些pipeline/task之间的依赖关系)


### Static Scheduling

DBMS在生成查询计划时决定使用多少线程来执行查询。

它在查询执行时不会改变。

- 最简单的方法是使用与核心数量相同的任务数量
- 仍然可以根据数据位置分配任务给线程，以最大限度地提高本地数据处理


### Morsel-Driven Scheduling

在被称为“marsels”的水平分区上运行的任务的动态调度，这些分区分布在核之间。

- 每个核心一个worker
- 每个任务一个morsel
- 基于pull的任务分配
- Round-robin data placement

支持并行的numa感知运算符实现。


### Hyper - Architecture

没有单独的dispatcher线程。

工作线程使用单个任务队列对每个查询计划执行协作调度。

- 每个worker尝试选择将在其本地片段上执行的任务
- 如果没有本地任务，那么worker只是从全局工作队列中提取下一个任务

### Morsel-Driven Sechduling

因为每个核心只有一个工作线程，每个任务只有一个任务，HyPer必须使用工作窃取，否则线程可能会闲置(sit idle)等待掉队者(stragglers)。

dbms使用lock-free哈希表来维护全局工作队列。

### Observation

每个元组的任务可以有不同的执行成本。

- 示例:Simple Selection(简单选择) vs. 字符串匹配

HyPer也没有执行优先级的概念。

- 所有查询任务执行方式相同
- short-running查询被long-running查询阻塞


### UMBAR - Morsel Scheduling 2.0

任务不是在运行时静态创建的。

每个任务可能包含多个片段。

stride(大步)调度的现代化实现.

优先级衰减(decay) (当查询出现时, 给予最高优先级, 如果运行的时间很长, 他的优先级成指数级下降)

> Fireblot: How we build fireblot


### UMBRA - Stride Scheduling

每个worker维护自己的线程本地元数据，这些元数据是关于要执行的可用任务的。

- Active Slots(活动槽):全局槽位数组中有活动任务集可用的表项
- Change Mask:表示有新的任务集加入到全局槽位阵列
- Return Mask:表示工作线程完成任务集的时间

worker对TLS元数据执行CAS更新以广播更改


当worker完成查询活动任务集的最后morsel时，它将下一个任务集插入到全局槽数组中，并更新所有工作线程的返回掩码。

> 这部分需要结合图示理解, 详见课件内容


### SAP HANA - NUMA-Aware Scheduler

使用组织成组(池)的多个工作线程进行基于pull的调度。

- 每个cpu可以有多个组(group)
- 每个group有一个soft and hard优先队列

使用一个单独的“watchdog”线程来检查group是否饱和(saturated)，并可以动态地重新(reassign)分配任务。


### SAP HANA - Thread Groups

DBMS维护每个线程组的软、硬优先级任务队列。

- 线程可以从其他组的软队列中获取任务

每个组有四个不同的线程池:

- Working:正在执行的任务

- Inactive:由于latch而阻塞在内核内部

- Free:休眠一会儿，醒来后看是否有新的任务要执行

- Parked:等待任务(如空闲线程)，但在内核中阻塞，直到看门狗线程唤醒它


### SAP HANA - NUMA-Aware Scheduler

根据任务是cpu绑定还是内存绑定，动态调整线程绑定(thread pinning)。

- 允许更多的跨区域窃取，如果dbms是cpu限制

SAP发现，对于sockets数量较多的系统，work-stealing并不那么有益

- HyPer(2-4个插槽)vs. HANA(64个插槽)

使用线程组允许内核执行其他任务，而不仅仅是查询。


### SQL Server - SQLOS

SQLOS是一个用户模式numa感知的操作系统层，它运行在DBMS内部并管理已配置(provisioned)的硬件资源。

- 确定哪些任务被调度到哪些线程上
- 还管理I/O调度和更高级的概念，如逻辑数据库锁

通过instrumented DBMS code的非抢占式线程调度。


### SQL Server - SQLOS

SQLOS的quantum(?)是4ms，但是调度器不能强制执行。

DBMS开发人员必须在源代码的不同位置添加显式yield调用。

Other Examples:
- ScyllaDB
- FaunaDB
- CoroBase

```
SELECT * FROM R WHERE R.val = ?

# Approximate Plan
for t in R:
    if eval(predicate, tuple, params):
        emit(tuple)

last = now()
for tuple in R:
    if now() - last > 4ms:
        yield
        last = now()
    if eval(predicate, tuple, params):
        emit(tuple)
```


### Observation

如果请求到达DBMS的速度比它执行请求的速度快，那么系统就会过载(overloaded)。

操作系统无法在这里帮助我们，因为它不知道线程在做什么:

- CPU绑定:不处理
- 内存绑定:OOM

最简单的DBMS解决方案: Crash


### Flow Control

方法1: 准入控制(Admission Control)

- 当系统认为没有足够的资源来执行新请求时中止新的请求

方法2:节流(Throttling)

- 延迟对客户端的响应，增加请求之间的时间间隔
- 这假设一个同步提交方案


### Parting Thoughts

我们忽略了磁盘I/O调度。

DBMS是一个美丽的、strong-willed的独立软件。但它必须正确使用硬件。

- 数据位置是一个重要的方面
- 在单节点DBMS中跟踪内存位置与在分布式DBMS中跟踪分片相同

Do not let the OS ruin your life.


### Next Class

Vectorized Query Execution
