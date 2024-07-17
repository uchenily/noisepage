## OLAP Indexes


### Last Class

我们讨论了列式存储的优势。

列数据库中的所有属性必须是固定长度才能启用偏移寻址。

如果dbms假定它的数据文件是不可变的，那么它将启用一些优化机会。(写一次, 读很多次, 永远不会对它进行在线更新)

#### Observation

OLTP dbms使用索引来查找**单个元组**，而无需执行顺序扫描。
- 基于树的索引(B+Tree)用于具有低选择性谓词的查询
- 还需要适应增量更新(不能假设文件是不可变的)

> 树节点中有一些额外空间需要用来容纳稍后的更新, 叶节点实际上是B+树中的任意节点, 现实世界中的数据库, 大约是70%满, 额外的30%是浪费的空间

但是OLAP查询**不一定需要找到单个元组**，并且**所有文件都是只读**的。

How can we speed up sequential scans(顺序扫描)?

> 这节课, 需要忘记之前所有关于索引的知识--特别是B+树


### Sequentail Scan Optimizations

- Data Prefetching
- Task Parallelization / Multi-threading
- Clustering / Sorting (聚类/排序)
- Late Materialization (晚物化)
- Materialized Views / Result Caching (物化视图, 是结果缓存的一种变体, 它需要更复杂的查询)
- Data Skipping
- Data Parallelization / Vectorization (使用向量化指令)
- Code Specialization / Complication (代码特化/编译生成机器码, 而不是解释执行)


今天的重点: Data Skipping


### Data Skipping

方法1:近似查询(有损)
- 对整个表的一个抽样子集执行查询，以产生近似的结果

例如:BlinkDB, Redshift, ComputeDB, XDB, Oracle, Snowflake, Google BigQuery, DataBricks

> 在一些应用领域, 实际上不需要获取确切的答案, 比如网站的确切访问人数

方法2:数据修剪(无损)
- 使用辅助数据结构来评估谓词，以快速识别DBMS可以跳过的表的部分，而不是单独检查元组。
- DBMS必须考虑**范围**vs**过滤效率**、**手动v**s**自动**之间的权衡。


### Data Considerations

Predicate Selectivity(谓词选择性)
- 有多少元组将满足查询的谓词

Skewness(倾斜?)
- 属性是否所有值都是唯一的，还是包含许多重复的值。

聚类/排序
- 表是否根据查询谓词访问的属性进行预排序。


### Today's Agenda

- Zone Maps
- Bitmap Indexes
- Bit-Slicing (bit切片)
- bit-Weaving
- Column Imprints
- column Sketches

### Zone Maps

预先计算元组块中属性值的聚合。DBMS首先检查区域映射，以决定是否要访问该块。

- 最初称为小物化聚集体(SMA)
- DBMS自动创建/维护这些元数据

```
# origin data
| 100 | 200 | 300 | 400 | 400 |

# zone map
| min | 100 |
| max | 400 |
| avg | 280 |
| sum | xxx |
| cnt | yyy |

现在有一条select 语句:
SELECT * FROM table WHERE val > 600;
先看一下zone map, 发现最大值就是400, 所以可以直接跳过
```

> Parquet: data organization 中metadata也包含min/max/count等

### Observation

范围与过滤效率之间的权衡。
- 如果scope太大，那么区域映射就没用了
- 如果范围很小，那么dbms将花费太多时间检查区域映射

区域映射仅在目标属性的位置和值相关联时才有用


### Bitmap Indexes

为一个属性的每个唯一值存储一个单独的位图，其中向量中的偏移量对应于一个元组。

- 位图中的第i个位置对应表中的第i个元组

通常分段成块，以避免分配大块的连续内存。

- 示例:PAX中的每行组一个

```
origin data
| id   | 1 | 2 | 3 | 4 | 5 |
| lit? | Y | Y | Y | N | Y |

compressed data
       | 1 | 1 | 1 | 0 | 1 |
```

### Bitmap Indexes: example

```
CREATE TABLE customer_dim (
  id INT PRIMARY KEY,
  name VARCHAR(32),
  email VARCHAR(64),
  address VARCHAR(64),
  zip_code INT
);

SELECT id, email FROM customer_dim WHERE zip_code IN (15216, 15217, 15218);


用三个位图的交集来查找匹配的元组

假设有 1000万 个元组, 43000 个zip code
- 10000000 x 43000 = 53.75 GB

这很浪费，因为位图中的大多数整体都是零
```

### Bitmap Index: design choices

编码方案
- 如何在位图中表示和组织数据

压缩
- 如何减少稀疏(sparse)位图的大小


### Bitmap Index: Encoding

方法#1:相等编码

- 每个唯一值一个位图的基本方案

方法#2:范围编码

- 每个间隔使用一个位图，而不是每个值使用一个
- 例如:PostgreSQL BRIN

方法#3:分层(Hierarchical)编码

- 使用树来识别空键(empty key)范围

方法#4:位切片编码

- 在所有值中使用每个位位置的位图


### Hierarchical Encoding

```
Offset: 1, 3, 9, 12, 13, 14, 38, 40

                        [1, 0, 1, 0]

   [1, 0, 1, 1]   [0, 0, 0, 0]   [0, 1, 0, 0]  [0, 0, 0, 0]

1010|0000|1001|1100
1 3       9  12 ...

全为0的部分可以不用存
```

> 这种方式缓存行不友好


### Bit-slice encoding

```
original data
| id      | 1     | 2     | 3     | 4     | 5     |
| zipcode | 21042 | 15217 | 02903 | 90220 | 53703 |


bin(21042) -> 00101001000110010
bin(15217) -> 00011101101110001

bit-slices
| null | 16 | 15 | 14 | 13 | ....
|   0  | 0  | 0  | 1  | 0  | ....
|   0  | 0  | 0  | 0  | 1  | ....
|   0  |
|   0  |


SELECT * FROM customer_dim WHERE zipcode < 15217;

只需要找前三位为零的(16, 15, 14三列)
```

位片也可以用于高效的聚合计算

示例:SUM(attr)使用Hamming Weight

- 首先，计算第17片中1的个数，然后乘以2^17
- 然后数出第16片中1的个数，并乘以2^16
- 对剩下部分重复这个步骤 ...

英特尔在2008年增加了POPCNT SIMD指令 (一次性或者上面步骤中的和)


### Bit-weaving

列式数据库的替代存储布局，旨在使用SIMD对压缩数据进行有效的谓词计算。

- 保序字典编码
- bit-level并行化
- 只需要通用指令(没有分散/聚集)

在威斯康星州的QuickStep引擎中实现。

- 2016年成为Apache孵化器项目, but then died in 2018.


### Bit-weaving - Storage Layouts

方法1:水平

- bit-level的面向行存储

方法2:垂直

- bit-level的面向列存储

### bit-weaving/H - example

```
原始数据:
t0 ... t7

排列成这样:
t0 t4 : 0001 0110
t1 t5 : 0101 0100
t2 t6 : 0110 0000
t3 t7 : 0001 0111

假设查询语句是:
SELECT * FROM table WHERE val < 5;

X = [0|0|0|1, 0|1|1|0]
Y = [0|1|0|1, 0|1|0|1]
第一个0是分隔符(delimiter)

mask = [0|1|1|1, 0|1|1|1]

(Y + (X xor mask)) | ~mask = [1|0|0|0, 0|0|0|0]
                              ^        ^
                              <5       >5

对于多个X, 都得到了一个结果, 比如是这样: (只有第一位/第四位可能为1/0, 其他位一定是0)
10000000 >> 0 10000000
00001000 >> 1 000011000
00001000 >> 2 0000001000
10000000 >> 3 00010000000

              vvvvvvvv
              10010110   -> 得到一个bitmap, 显示实际匹配的元组(即val < 5)
                            也叫选择向量
```

计算一个word只需要三个指令
适用于任何word大小



### Selection Vector

SIMD比较运算符产生一个位掩码，用于指定哪些元组满足谓词。

- DBMS必须将其转换为列偏移量(column offsets)

方法1:迭代

方法2:预先计算的位置表 (Pre-computed Positions Table)


```
selection vector
t0 t1 t2 ... t7
1 0 0 1 0 1 1 0 => 150

# 迭代
tuples = []
for (i=0; in; i++) {
    if sv[i] == 1 {
        tuples.add(i)
    }
}

# Positions Table
| key | payload |
| xxx | ...
| yyy | ...
| 150 | ...
| ...
```

### Vertical Storage

```
t0 001
t1 101
t2 110
t3 001
t4 110
t5 100
t6 000
t7 111

t8 100
t9 011

=>

segment #1           segment #2
   t0 .. t7          t8t9 ...
v0 01101101       v3 10000000
v1 00101001       v4 01000000
v2 11010001       v5 01000000

   ^^^^^^^^          ^^^^^^^^
   processor word
```

### Bit-Weaving/V - example

```
SELECT * FROM table WHERE val = 2;
                                ^
                               010

segment #1
   t0 .. t7
v0 01101101
v1 00101001
v2 11010001

SIMD compare #1: 00000000 xor 01101101 = 10010010
SIMD compare #2: 10010010  |  00101001 = 00000000
SIMD compare #3: no need any more
```

可以像位图索引一样执行早期修剪(pruning)

最后一个向量被跳过，因为前面比较的所有位都是零


### Observation

所有以前的位图方案都是关于存储列数据的精确/无损表示。

在一般情况下，DBMS可以放弃一些准确性，以换取更快的评估速度。

- 它仍然必须始终检查原始数据，以避免误报(false positive)。


### Column Imprints

存储一个位图，该位图指示在cacheline值的位片上是否设置了比特位。

```
original data
| 1 | 8 | 4 |

bitmap indexes
1 2 3 4 5 6 7 8
---------------
1 0 0 0 0 0 0 0
0 0 0 0 0 0 0 1
0 0 0 1 0 0 0 0

column imprint (将上面的bitmap indexes压缩, 和布隆过滤器比较像)
1 0 0 1 0 0 0 1
```


### Column Sketches

一种范围编码位图的变体，它使用较小的草图编码来指示元组的值存在于一个范围内。

DBMS必须自动找出code的最佳映射。

- 值的分布和紧凑性之间的权衡(trade-off)
- 为频繁值分配唯一code，避免误报

```
original data
| 13 | 191 | 56 | 92 | 81 | 140 | 231 | 172 |  (8bits)


histogram (直方图)
|   00   |   01    |   10    |    11    |
^        ^         ^         ^          ^
0        60        132       170        256

Compression Map
60  --> 00
132 --> 01
170 --> 10
256 --> 11

sketched column
| 00 | 10 | 11 | 00 | 01 | 01 | 10 | 11 |


现在有这样一个query:
SELECT * from table WHERE val < 90;

从Compression Map上找, map(90)=01, 只有 00/01 两种情况, 可以根据这个条件过滤一部分数据
```

### Parting Thoughts

区域映射(Zone Maps)是最广泛使用的加速顺序扫描的方法。

位图索引(Bitmap Indexs)在NSM DBMS中比列式OLAP系统更常见。

我们忽略了多维索引和倒排索引…


### Next Class

Data Compression (Tuples, Indexes)


### Project #1 - Foreign data wrapper

您将为PostgreSQL创建一个外部数据包装器，用于在自定义列数据格式上使用谓词下推(predicate pushdown)执行简单扫描。

目的是让您熟悉扩展Postgres和编写简单的扫描算子(scan operator)

[Project #1 - Foreign Data Wrapper - CMU 15-721 :: Advanced Database Systems (Spring 2023)](https://15721.courses.cs.cmu.edu/spring2023/project1.html)

### Project #1 - Tasks

我们提供了一种简单的列数据文件格式(没有做任何压缩)。为这个文件编写一个解析器，它可以扫描单个列并将元组拼接在一起。

将其作为外部表与Postgres集成。

增加对谓词计算(predicate evaluation)的支持。


### Development Hints

首先单独编写基本解析器代码，然后将其连接到Postgres。

我们建议用C++, 使用PGXS完成这个项目

编写您自己的本地测试。


### Things to Note

除了提交给Gradescope的文件外，不要更改其他文件。

一定要fork我们的Postgres版本。

### Plagiarism Warning (剽窃警告)

Your project implementation must be your own work.
- You may not copy source code from other groups or the web.

Plagiarism will not be tolerated.
