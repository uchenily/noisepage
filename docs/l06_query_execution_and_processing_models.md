### Last Class

上两节课讲的是在执行顺序扫描时，如何使DBMS处理的数据量最小化(minimize)。

现在我们将开始讨论改进DBMS查询执行性能的方法。

### Execution Optimization

DBMS工程是一系列优化的编排，旨在充分利用硬件。没有哪一种技术比其他技术更重要。

Andy's Unscientific Top-3 Optimizations:

- 数据并行化(矢量化)
- 任务并行化(多线程)
- 代码专门化(编译)

### Optimization Goals

方法1: 减少指令计数

- 用更少的指令完成同样的工作

方法2: 减少每条指令的周期

- 在更少的周期内执行更多的CPU指令

方法#3: 并行执行

- 使用多个线程并行计算每个查询


### Query Execution

查询计划是operator的DAG(有向无环图)
> 先忽略它是物理operator还是逻辑operator
> operator例子: join operator, scan operator

operator实例是对唯一数据段的operator调用

task是一个或多个operator实例的序列(有时也称为pipeline)

```
SELECT A.id, B.value
  FROM A JOIN B
    ON A.id = B.id
 WHERE A.value < 99
   AND B.value > 100

        π  
        ω  pipeline #2: 扫描B, 过滤, hash join (必须在pipeline #1完成后才开始执行pipeline #2)
   σ         σ
A               B

pipeline #1: 扫描A, 过滤, 产生输出
```


### Today's Agenda

- MonetDb/X100 Analysis
- Processing Models
- Parallel Execution

### MonetDB/X100 (2005)

对OLAP工作负载上内存DBMS的执行瓶颈进行低级(low-level)分析。

- 展示DBMS是如何不正确地设计现代CPU架构的

> 人类经常以容易推理和理解的方式构建数据库系统或一般的软件系统, 当通常用人类更容易理解的方式去实现, 实际上是在cpu上运行它的最糟糕的方式

基于这些发现，他们提出了一种名为MonetDB/X100的新型DBMS

- 更名为Vectorwise并于2010年被Action收购
- 更名为Vector和Avlanche

### CPU overview

cpu将指令组织到流水线阶段(pipeline stages)
目标是通过屏蔽无法在单个周期内完成的指令的延迟，使处理器的所有部分在每个周期内都保持繁忙(busy)。

超标量cpu支持多个管道。

- 如果多个指令是独立的，则在一个周期内并行执行(乱序执行 out-of-order execution)。

一切都很快，直到有一个错误…


### DBMS / CPU problems

问题1: 依赖关系

- 如果一个指令依赖于另一个指令，那么它不能立即被推入同一pipeline

问题2: 分支预测

- CPU试图预测哪个分支程序的指令将采取和填充管道(take and fill in the pipeline)
- 如果它出错了，它必须扔掉任何投机工作(speculative work)并flush pipeline (即回滚)


### Branch Misprediction

由于长pipeline，cpu将推测性地(speculatively)执行分支。这可能隐藏了依赖指令之间的长停顿。

DBMS中执行最多的分支代码是**顺序扫描期间的筛选操作**。

但这是(几乎)不可能正确预测的。


### Selection Scans

```
SELECT * FROM table WHERE key > $(low) AND key < $(high);
```

```
# Scalar (Branching)
i = 0
for t in table:
    key = t.key
    if (key > low) && (key < high):
        copy(t, output[i])
        i = i + 1
```

```
# cpu更友好, 尽管copy次数更多
# Scalar (Branchless)
i = 0
for t in table:
    copy(t, output[i])
    key = t.key
    delta = (key>low ? 1 : 0) & (key < high ? 1 : 0)
    i = i + delta
```


### Excessive Inustructions(过多的指令)

DBMS需要支持不同的数据类型，因此它必须在对值执行任何操作之前检查值类型。

- 这通常实现为巨大的switch语句
- 也会产生更多的分支，CPU很难可靠地预测

示例:Postgres' addition for NUMERIC types (int PGTYPESnumeric_add(...))


### Processing Model

DBMS的处理模型定义了系统如何执行查询计划。

- 不同的工作负载权衡(OLTP与OLAP)。

方法1:迭代器模型

方法2:物化模型 (可能更适合OLTP系统)

方法#3:矢量化/批处理模型


### Iterator Model

每个查询计划操作符实现一个next函数。

- 每次调用时，操作符返回单个元组，如果没有元组，则返回空标记。
- 操作符实现一个循环，调用子操作符的next来获取它们的元组，然后处理它们。

也叫火山(Volcano)模型或管道模型。


```
SELECT R.id, S.cdate
  FROM R JOIN S
    ON R.id = S.id
 WHERE S.value > 100

        π  R.id, S.cdate (s1)
        ω  R.id=S.id (s2)
            σ  value > 100 (s4)
R (s3)          S (s5)

# s1
for t in child.next():
    emit(projection(t))

# s2
for t1 in left.next():
    build_hash_table(t1)
for t2 in right.next():
    if probe(t2):
        emit(t1 ω t2)

# s4
for t in child.next():
    if eval_pred(t):
        emit(t)

# s3
for t in R:
    emit(t)

# s5
...
```


这在几乎所有DBMS中都使用。允许元组流水线。

有些操作符必须阻塞，直到其子操作符发出所有元组。

- joins, subqueries, order by

使用这种方法输出控制很容易


### Materialization Model

每个operator一次处理所有输入，然后一次emit(发出)所有输出

- operator将其输出“物化”为单个结果
- DBMS可以下推提示(push down hints)(例如:LIMIT)以避免扫描过多的元组
- 可以发送一个物化的行或单个列

输出可以是整个元组(NSM)或列的子集(DSM)


```
SELECT R.id, S.cdate
  FROM R JOIN S
    ON R.id = S.id
 WHERE S.value > 100

        π  R.id, S.cdate (s1)
        ω  R.id=S.id (s2)
            σ  value > 100 (s3)
R (s4)          S (s5)

# s1
out = []
for t in child.output():
    out.add(projection(t))
return out

# s2
out = []
for t1 in left.output():
    build_hash_table(t1):
for t2 in right.output():
    if probe(t2):
        out.add(t1 ω t2)
return out

# s4
out = []
for t in child.output():
    if eval_pred(t):
        out.add(t)
return out

# s3
out = []
for t in R:
    out.add(t)
return out

# s5
...
```

更适合OLTP工作负载，因为查询一次只访问少量元组

- 降低执行/协调开销
- 更少的函数调用

对于具有大量中间结果的OLAP查询来说不是很好


> 小结:
> 迭代器模型: 一次一个元组
> 物化模型: 一次所有的元组
> 折中方案 --> 向量化模型


### Vectorization Model

就像迭代器模型，每个操作符实现一个next函数，但是…

每个操作符发出(emit)一批(batch)元组，而不是单个元组

- 操作符的内部循环一次处理多个元组
- 批量大小根据硬件或查询属性不同而不同

```
SELECT R.id, S.cdate
  FROM R JOIN S
    ON R.id = S.id
 WHERE S.value > 100

        π  R.id, S.cdate (s1)
        ω  R.id=S.id (s2)
            σ  value > 100 (s3)
R (s4)          S (s5)

# s1
out = []
for t in child.next():
    out.add(projection(t))
    if |out| > n:
        emit(out)

# s2
out = []
for t1 in left.next():
    build_hash_table(t1):
for t2 in right.output():
    if probe(t2):
        out.add(t1 ω t2)
    if |out| > n:
        emit(out)

# s4
out = []
for t in child.output():
    if eval_pred(t):
        out.add(t)
    if |out| > n:
        emit(out)

# s3
out = []
for t in R:
    out.add(t)
    if |out| > n:
        emit(out)

# s5
...
```

非常适合OLAP查询，因为它大大减少了每个operator的调用次数。

允许operator更容易地使用矢量化(SIMD)指令来处理批量元组。


### Observation

在前面的示例中，dbms从查询计划的root开始，并从叶子操作符中提取数据。
>从上到下的方式, 也叫pull-based方式(pull子节点的数据到当前节点)

这是大多数dbms实现其执行引擎的方式。


### Plan Processing Direction

方法1:从上到下(pull)

- 从根节点开始，从它的子节点“拉”数据
- 元组总是与函数调用一起传递

方法2:从下到上(push)

- 从叶节点开始，将数据“推送”到它们的父节点
- 我们会在HyPer和Peloton的ROF中看到


### Push-based Iterator Model

```
SELECT R.id, S.cdate
  FROM R JOIN S
    ON R.id = S.id
 WHERE S.value > 100

        π  R.id, S.cdate
        ω  R.id=S.id
            σ  value > 100
R              S

# pipeline #1: scan R
# pipeline #2: scan S + apply pred + probe hashtable + projection

# p1
for t1 in R:
    build_hash_table(t1)

# p2
for t2 in S:
    if eval_pred(t):
        if probe_hash_table(t2):
            emit(projection(t1 ω t2))  
```
> 假设能够通过编译生成这样的代码

### Plan Processing Direction

方法1: 从上到下(pull)

- 易于通过LIMIT控制输出
- 父操作符阻塞，直到其子操作符返回一个元组
- 额外的开销，因为操作符的next函数是作为虚函数实现的
- 每次next调用的分支成本

方法2: 从下到上(push)

- 允许对流水线中的缓存/寄存器进行更严格的控制
- 难以通过LIMIT控制输出
- 难以实现Sort-Merge Join


### Parallel Execution

DBMS同时执行多个任务以提高硬件利用率。

- active任务不需要属于同一查询

方法#1:查询间(inter-query)并行

方法2:查询内(intra-query)并行


这些技术并不相互排斥。(exclusive)

每个关系运算符都有并行算法。


### Intra-Operator Parallelism

方法1:Intra-Operator(横向)

- 操作符被分解为独立的实例，在不同的数据子集上执行相同的功能

DBMS在查询计划中插入一个交换(exchange)操作符来合并(coalesce)子操作符的结果


```
SELECT A.id, B.value
  FROM A JOIN B
    ON A.id = B.id
 WHERE A.value < 99
   AND B.value > 100

        π
        ω
    σ        σ
A               B

--------------------

      exchange

htable htable htable

σ      σ      σ

A1     A2     A3

core1  core2  core3
```


### Inter-Operator Parallelism

方法2: Inter-Operator(垂直)

- 操作是重叠的，以便将数据从一个阶段输送到下一个阶段，而不会具体化
- worker同时执行来自查询计划的不同段的多个操作符
- 仍然需要交换运营商从部分合并中间结果

也称为流水线并行(pipelined parallelism)

> 有点像生产者/消费者模式


```
SELECT *
  FROM A
  JOIN B
  JOIN C
  JOIN D


     ω
 ω       ω
A  B    C  D

-----------------------------
          exchange
   ω                   ω
(core 3)             (core 4)
-----------------------------
exchange            exchange
   ω                   ω    
  A  B                C  D
(core 1)             (core 2)
```

> 注: 这里没有where语句, 所有join都是笛卡尔积, 所以可以完全并行地处理


### Parting Thoughts

对于现代cpu来说，实现某些东西的最简单方法并不总是产生最有效的执行策略。

我们将看到矢量化/自底向上执行将是执行OLAP查询的更好方式。

### Next Class

Query Task Scheduling
