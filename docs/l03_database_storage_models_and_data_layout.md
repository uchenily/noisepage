## Storage Models & Data Layout

今天的讲座是关于数据库中数据的最低物理表示。
数据的“外观”几乎决定了DBMS的整个系统架构。

- 加工模型
- 元组物化策略
- 算子算法
- 数据摄取/更新
- 并发控制(我们将忽略这一点)
- 查询优化

### Storage Models
DBMS的存储模型指定了它如何在磁盘和内存中物理地组织元组。

选择#1:n元存储模型(NSM)
选择#2:分解存储模型(DSM)
选择3:混合存储模型(PAX)

#### N-ary Storage Model (NSM)

dbms将(几乎)单个元组的所有属性连续存储在单个页面中。

非常适合OLTP工作负载，其中txns倾向于访问单个实体和插入繁重的工作负载。
- 使用一次元组**迭代器处理模型**。

NSM数据库页面大小通常是**4KB**硬件页面的常数倍。
- 示例:Oracle(4KB)， Postgres(8KB)， MySQL(16KB)

#### NSM: Physical Organization

面向磁盘的NSM系统将元组的定长和变长属性连续地存储在单个槽页中。

元组的记录id (page#， slot#)是DBMS唯一标识物理元组的方式。

> 对于 `SELECT SUM(colA), AVG(colC) FROM xxx WHERE colA > 1000`, 使用这种方式会导致内存读取位置跳来调去(不确定, 因为需要先根据slot array找到header, 然后根据header找到具体的列, 对缓存不友好(非顺序读取); 实际上不需要读取colB, 但是这种存储方式下必须把colB读取到内存)

优势:
- 快速插入、更新和删除
- 适用于需要整个元组(OLTP)的查询
- 可以使用面向索引的物理存储进行集群
劣势:
- 不适合扫描表的大部分和/或属性的子集
- 访问模式中糟糕的内存局部性
- 不适合压缩，因为在一个页面中有多个值域


### Decomposition Storage Model (DSM) -> 其实就是列式存储
dbms在数据块中连续存储所有元组的单个属性。

理想的OLAP工作负载，其中只读查询对表的属性子集执行大量扫描。
- 使用批量**矢量化处理模型**。

文件大小更大(100 mb)，但它仍然可以将文件中的元组组织成更小的组。


#### DSM: Physical Organization
存储属性和元数据(例如:空值)在**固定长度**值的单独数组中。
- 大多数系统使用这些数组的偏移量识别唯一的物理元组。
- 需要处理可变长度的值…

为每个属性维护一个单独的文件，并为整个列的元数据提供专用的头文件。

```
| header | null bitmap | a1 | a2 | a3 | ... |

| header | null bitmap | b1 | b2 | b3 | ... |
```

> null bitmap 是为了预留空间


#### DSM: Tuple Identification

选择#1:固定长度的偏移量
- 每个属性值的长度相同。

选择#2:嵌入元组id (实际基本没用)
- 每个值与元组id一起存储在一个列中。


#### DSM: variable-length data

填充可变长度字段以确保它们是固定长度是浪费的，特别是对于大属性。

更好的方法是使用**字典压缩**将重复的变长数据转换为固定长度的值(通常是32位整数)。
- 下周将详细介绍

#### DSM: system history
1970s: Cantor DBMS
1980s: DSM Proposal
1990s: SybaseIQ(in-memory only)
2000s: Vertica, Vectorwise, MonetDB
2010s: Everyone


优势:
- 减少每次查询浪费的IO数量，因为dbms只读取它需要的数据
- 更快的查询处理，因为增加了局部性和缓存数据重用。
- 更好的数据压缩(稍后会详细介绍)

劣势:
- 由于元组分割/拼接/重组，点查询、插入、更新和删除速度较慢

#### Observation
OLAP查询几乎从不单独访问表中的单个列。
- 在查询执行过程中的某个时刻，dbms必须获得其他列并将原始元组拼接在一起。

但是我们仍然需要以列格式存储数据，以获得存储+执行的好处。

我们需要列式模式，它仍然单独存储属性，但使每个元组的数据在物理上彼此接近……


### PAX Storage Model
跨分区属性(PAX)是一种混合存储模型，它垂直划分数据库页面中的属性。
- This is what Parquet and Orc use.

我们的目标是在保留行存储的空间局部性(spatial locality)优势的同时，获得在列存储上更快处理的好处。

#### PAX: Physical Organization

水平将行划分为组。然后将它们的属性垂直划分为列。

全局标头包含目录，其偏移量为文件的行组。
- 如果文件是不可变的，则存储在页脚(Parquet, Orc)

每个行组包含关于其内容的自己的元数据标头。

```
| header | a0 | a1 | a2 | b0 | b1 | b2 | c0 | c1 | c2 |
|                    row-group-0                      |

| header | a3 | a4 | a5 | b3 | b4 | b5 | c3 | c4 | c5 |
|                    row-group-1                      |
```


### Memory Pages
OLAP dmbs使用我们在介绍课程中讨论过的缓冲池管理器方法。

操作系统将物理页面映射到虚拟内存页面。

cpu的MMU维护一个包含虚拟内存页的物理地址的TLB。
- TLB位于CPU缓存中。
- 它显然不能存储大内存机器的所有可能条目。

当您分配内存块时，分配器保持它与页面边界对齐


#### Transparent Huge Pages (THP)
Linux支持创建更大的页(2MB到1GB)，而不是总是按4KB页分配内存。
- 每个页面必须是一个连续的内存块
- 大大减少TLB表项的数量

使用THP，操作系统在后台重新组织页面以保持内容紧凑
- 将较大的页面拆分为较小的页面
- 将较小的页面合并为较大的页面
- 可能导致DBMS进程的内存访问停滞

从历史上看，每个dbms都建议你在linux上禁用这个THP:
- Oracle, SingleStore, NuoDB, MongoDB, Sybase, TiDB
- Vertica说只有在较新的linux发行版中才启用THP

谷歌最近的研究表明，庞大的页面将他们的数据中心工作量提高了7%
- Sapnner的吞吐量提高6.5%

#### Observation
当数据进入数据库时，它是“热的”
- 新插入的元组更有可能在不久的将来再次更新。

随着年龄的增长，它的更新频率降低了
- 在某些情况下，元组只能在只读查询中与其他元组一起访问

### Hybrid Storage Model
使用针对NSM或DSM数据库进行优化的单独执行引擎。
- 将新数据存储在NSM中，实现快速OLTP
- 将数据迁移到DSM，以实现更高效的OLAP
- 将来自两个引擎的查询结果组合在一起，在应用程序中显示为单个逻辑数据库

Choice #1: Fractured Mirrors
- Examples: Oracle, IMB DB2 Blu, Microsoft SQL Server
Choice #2: Delta Store
- Examples: SPA HAHA, Vertica, SingleStore, Databrick, Google Napa

#### Fractured Mirrors
在自动更新的DSM布局中存储数据库的第二个副本
- 所有更新首先在NSM中输入，然后最终复制到DSM镜像
- 如果DBMS支持更新，则必须使DSM镜像中的元组失效

#### Delta Store
在NSM表中对数据库进行状态更新。
后台线程从增量存储中迁移更新并将其应用于DSM数据。
- 批量处理大块，然后将它们作为PAX文件写出来。


### Database Partitioning

跨多个资源拆分数据库:
- 磁盘、节点、处理器
- 在NoSQL系统中通常称为“sharding”(分片)

dbms在每个分区上执行查询片段，然后组合结果生成单个答案。

dbms可以对数据库进行物理分区(不共享)或逻辑分区(共享磁盘)。


#### Horizontal Partitioning

根据某些分区键和模式将表的元组拆分为不相交的子集
- 选择在大小、负载或使用方面均等地划分数据库的列

分区方案:
- 哈希
- 范围
- 谓词

#### Parting Thoughts
每个现代OLAP系统都在使用PAX存储的某种变体。关键思想是所有数据必须是**固定长度**的。

现实世界中的表主要包含数字属性(int/float)，但它们占用的存储主要由字符串数据组成。

现代列式系统是如此之快，以至于大多数人都不会对数据仓库模式进行反规范化


#### Next class

如何使用辅助数据结构加速对列数据的OLAP查询。
- Zone Maps
- BitMap Indexes
- Sketches

我们还将讨论项目#1
