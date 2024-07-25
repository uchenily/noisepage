## Query Complication & JIT Code Generation


### Last Class

如何使用SIMD向量化核心数据库算法进行顺序扫描。

- 查询内并行(intra-query parallelism)

10年前的研究文献给人的印象是矢量化和JIT编译是相互排斥的。
> 事实并非如此, 不过这节课将专注于编译查询, 不关心矢量化相关的东西


### Optimization Goals

方法1:减少指令计数 (这节课主要是介绍这种技术)

- 用更少的指令完成同样的工作

方法2:减少每条指令的周期

- 在更少的周期内执行更多的CPU指令

方法#3:并行执行

- 使用多个线程并行计算每个查询


### Microsoft Remark

在最小化查询执行期间的磁盘I/O之后，提高吞吐量的唯一方法是减少执行的指令数量。


### Today's Agenda

Background
Code Generation / Transpilation
JIT Compilation
Real-world Implementations
Project #3


### Observation

实现这种指令减少的一种方法是通过代码专门化(code specialization)。

这意味着生成特定于DBMS中某个任务的代码(eg: 一个查询)

大多数代码都是为了便于人们理解而编写的，而不是为了性能。


### Example Database

```
CREATE TABLE A (
    id INT PRIMARY KEY,
    val INT
);

CREATE TABLE B (
    id INT PRIMARY KEY,
    val INT
);

CREATE TABLE C (
    a_id INT REFERENCES A(id), # 外键
    b_id INT REFERENCES B(id), # 外键
    PRIMARY KEY (a_id, b_id)
);
```

### Query Interpretation(解释)

```
SELECT *
  FROM A, C,
   (SELECT B.id, COUNT(*)
      FROM B
     WHERE B.val = ? + 1
     GROUP BY B.id) AS B
  WHERE A.val = 123
    AND A.id = C.a_id
    AND B.id = C.b_id
```

> TODO: 课件图示


### Code Specialization

DBMS为任何对不同输入具有相似执行模式的cpu密集型任务生成代码。

- 接入方式

- 存储过程

- 查询运算符执行

- 谓词求值 (最常见的情况)

- 日志操作 (目前仅学术研究)


```
# 谓词求值
SELECT * FROM A
 WHERE val = 1;

对应的Tree
              Op(=)

Attribute(A.val)      Constant(1)

想要生成的函数:
bool check(val) {
    return val == 1;
}
```

方法#1:翻译(Transpilation, 也叫source-to-source编译)

编写代码，将关系查询计划转换为命令式语言源代码(source code)，然后通过常规编译器运行它以生成native code

方法2:JIT编译

生成查询的中间表示(IR, intermediate representation)，然后DBMS将其编译为native code

### Architecture Overview

![arch.png (1061×615) (raw.githubusercontent.com)](https://raw.githubusercontent.com/uchenily/noisepage/main/resources/arch.png)


### Hique - Code Generation

对于给定的查询计划，创建一个C/ c++程序来实现该查询的执行。

- 嵌入所有谓词和类型转换

使用现成的编译器将代码转换为共享对象，将其链接到DBMS进程，然后调用exec函数。


### Hique - Operator Templates

```
SELECT * FROM A WHERE A.val = ? + 1;
```

```
# interpreted plan
for t in range(table.num_tuples):
    tuple = get_tuple(table, t)
    if eval(predicate, tupe, params):
        emit(tuple)

# get_tuple(xxx):
# 1. Get schema in catalog for table
# 2. Calculate offset based on tuple size
# 3. Return pointer to tuple
```

```
# templated plan
tuple_size = ###
predicate_offset = ###
parameter_value = ###

for t in range(table.num_tuples):
    tuple = table.data + t * tuple_size
    val = (tuple + predicate_offset)
    if (val == parameter_value + 1):
        emit(tuple)
```


### Hique - DBMS Integration

生成的查询代码可以调用DBMS中的任何其他函数。这允许它使用所有相同的组件作为解释查询。

- Network handlers
- Buffer Pool Manager
- Concurrency Control
- Logging / Checkpoints
- Indexes

调试相对容易，因为您可以逐步执行生成的源代码。


### Hique - Evaluation

通用的迭代器

- 具有通用谓词计算的规范模型

优化的迭代器

- 带有内联谓词的特定类型迭代器

通用硬编码

- 使用泛型迭代器/谓词的手写代码

优化的硬编码

- 直接访问元组与指针算术

HIQUE

- 查询专用代码


### Observation

关系运算符是推断查询的有用方法，但不是执行查询的最有效方法。

将C/C++源文件编译成可执行代码需要(相对)较长的时间。

HIQUE也不支持完全的流水线。


### HYPER - JIT Query Compilation

使用LLVM toolkit将内存中的查询编译为native code。

- HyPer不是发出c++代码，而是发出(emit)LLVM IR。

管道中的aggressive运算符函数使元组尽可能长时间地保存在CPU寄存器中。

- Push-based vs. Pull-based
- 以数据为中心vs.以operator为中心


### Push-based Execution

```
SELECT *
  FROM A, C
   (SELECT B.id, COUNT(*)
      FROM B
     WHERE B.val = ? + 1
     GROUP BY B.id) AS B
 WHERE A.val = 123
   AND A.id = C.a_id
   AND B.id = C.b_id
```

```
# generated query plan

#1
for t in A:
    if t.val = 123:
        Materialize t in HashTable x(A.id=C.a_id)

#2
for t in B:
    if t.val == <param> + 1:
        Aggregate t in HashTable y(B.id)

#3
for t in y(B.id):
    Materialize t in HashTable x(B.id=C.b_id)

#4
for t3 in C:
    for t2 in x(B.id=C.b_id):
        for t1 in x(B.id=C.a_id):
            emit(t1 x t2 x t3)
```

### Query Compilation Cost

HyPer的查询编译时间相对于查询大小呈超线性增长。

- join的数量
- predicate的数量
- aggregation的数量

对于OLTP应用程序来说，这不是一个大问题。

OLAP工作负载的主要问题。


### Hyper - Adaptive Execution (自适应执行)

为查询生成LLVM IR，并使用解释器立即开始执行IR。

然后由DBMS在后台编译查询。

当编译的查询准备好时，无缝地替换解释执行。

- 对于每个morsel，检查编译后的版本是否可用

```
sql query              query plan
---> Optimizer (0.2ms) --> Code Generator (0.7ms)

LLVM IR
--->  ByteCode Compiler(0.4ms) --> byte code
---> Unoptimized LLVM Compiler (6ms) -> x86 code
---> LLVM Passes(25ms) ---> Optimized LLVM Compiler (17ms) -> x86 code
```


### Real-world Implementations

**Custom**
- IBM System R
- Action Vector
- Amazon Redshift (有巨大的查询缓存, 几乎有能执行的所有可能query)
- Oracle
- Microsoft Hekaton
- SQLite (Vritual Database engine: VDBE)
- TUM Umbra

**JVM-based**
- Apache Spark
- Neo4j
- Splice Machine (dead)
- Presto / Trino

**LLVM-based**
SingleStore (MemSQL Programming Language: MPL)
VitesseDB
PostgreSQL (2018, 只是编译表达式和where子句, 而不是像hyper一样的整体查询编译)
CMU Peloton (dead)
CMU NoisePage (dead)


```
# cache a query

SELECT * FROM A WHERE A.id = 123

=> 抽取出常量

SELECT * FROM A WHERE A.id = ?

用参数化的sql 字符串替换, 然后编译

现在如果有另一个查询, 只是参数的值不同, 那么也可以直接利用这个缓存
```

### Pleoton (2017)

使用LLVM对整个查询计划进行HyPer-style的完整编译。

Relax the pipeline breakers，为operators创建可矢量化的小批量。

使用软件预取(pre-fetching)来隐藏内存延迟


### NoisePage (2019)

SingleStore-style 的将查询计划转换为面向数据库的DSL

然后将DSL编译成操作码

进行HyPer-style的对操作码解释, 同时使用LLVM在后台编译


```
SELECT * FROM foo
 WHERE colA >= 50
   AND colB < 100000;

==>

fun main() -> int {
    var ret = 0
    for (row in foo) {
        if (row.colA >= 50 and row.colB < 100000) {
            ret = ret + 1
        }
    }
    return ret
}

==> 生成opcode

要么通过Interpreter去执行, 要么通过Optimized LLVM Compiler去执行
```

### Parting Thoughts

查询编译会很有帮助，但实现起来并不容易。

2016版的MemSQL是目前最好的查询编译实现。

任何想要竞争的新DBMS都必须实现查询编译。


### Next Class

Vectorization vs. Compilation
