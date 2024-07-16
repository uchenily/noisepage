## Modern Anayltical Database Systems

### 课程大纲

high-level 概述

Storage
- 列式存储
- 压缩
- 索引

Query Execution
- Processing Models
- Scheduling
- 向量化
- 编译
- Joins
- 物化视图(Materialized Views)
Query Optimization

Network Interfaces


### 今日日程

- Query Execution
- Distributed System Architectures
- OLAP Commoditization


### 数据分类

持久数据

中间数据 (一般会保存到内存)

怎样开始执行, 有两种策略:
- push query to data
- pull data to query

> 一个例子, 通过where语句, 从s3进行预筛选


### 系统架构

选择一: 不共享

选择二: 共享磁盘 (重点)
- scale compute layer independently from the storage layer
- easy to shutdown idle compute layer resources
- may need to pull uncached persistent data from storage layer to compute layer before applying filters.

云对象存储 -- 几乎无限扩展能力


### Object Stores

将数据库的表(持久数据)划分为存储在对象存储中的大型不可变文件。
- 一个元组的所有属性都存储在一个列式布局(PAX)的同一个文件中。
- Header(或footer)包含关于列偏移量、压缩方案、索引和区域映射的元数据。

DBMS检索块的头以确定需要检索的字节范围(如果有的话)。

每个云供应商都提供自己专有的API来访问数据(PUT、GET、DELETE)。
- 一些厂商支持谓词下推(S3)


### 其他主题

文件格式

表分区

数据摄取/更新/发现

调度/适应性


### Observation

Snowflake是一个完全由内部构建的组件组成的整体系统。

本学期我们将介绍的大多数非学术性dbms都具有类似的总体体系结构。

但这意味着多个组织正在编写相同的DBMS软件.


### OLAP commoditization(商品化)

趋势, engine子系统 -> 单独的开源组件

示例:
- System Catalogs (系统目录)
- Query Optimizers
- File Format / Access Libraries
- Execution Engines


#### System Catalogs

DBMS跟踪其目录中的数据库模式(表、列)和数据文件
- 如果DBMS在数据摄取路径(ingestion path)上，那么它可以增量地维护目录
- 如果外部进程添加了数据文件，那么它也需要更新目录，以便DBMS知道它们

著名的实现:
- HCatalog
- Google Data Catalog
- Amazon Glue Data Catalog

#### Query Optimizers
用于启发式和基于成本的查询优化的可扩展搜索引擎框架
- DMBS提供转换规则和成本估算
- 框架返回逻辑或物理查询计划

**这是在任何dbms中最难构建的部分**

著名的实现:
- Greenplus Orca
- Apacha Calcite

#### File Formats
大多数dbms为其数据库使用专有的磁盘二进制文件格式。在系统之间共享数据的唯一方法是将数据转换为通用的基于文本的格式
- 例如:csv、json、xml

有一些开源二进制文件格式，可以更容易地跨系统访问数据，并从文件中提取数据
- 库提供了一个迭代器接口来检索(批处理)文件中的列

通用格式:

- Apache Parquet(2013):来自Cloudera/Twitter的压缩列式存储
- Apache Iceberg(2017):灵活的数据格式，支持来自Netflix的模式演变。
- Apache ORC(2013):来自Apache Hive的压缩列式存储。
- Apache CarbonData(2013):使用华为索引的压缩列式存储。
- DHF5(1998):科学工作负载的多维阵列。
- Apache Arrow(2016):内存压缩列式存储，来自Pandas/??


#### Execution Engines
用于在列数据上执行向量化查询操作符的独立库。
- 输入是物理操作符(physical operators)的DAG。
- 需要外部调度和编排(orchestration)。

著名的实现:
- Velox
- DataFusion
- Intel OAP


#### Conclusion
今天的主题是了解现代OLAP dbms的高级上下文。
- 从根本上说，这些新的dbms与以前的分布式/并行dbms没有什么不同，除了基于云的对象存储共享磁盘的流行。

本学期剩下的时间，我们的重点将放在这些系统组件的最先进的实现上。

#### next class

- Storage Models
- Data Representation
- Partitioning
- Catalogs
