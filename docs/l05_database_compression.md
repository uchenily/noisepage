## Database Compression


### Last Class

我们讨论了dbms通过filtters(zone maps)和indexes 跳过olap查询中顺序扫描数据的方法


### Real-world Data Characteristics

数据集倾向于属性值的高度**倾斜(skewed)**分布。

- 示例:Brown Corpus(语料库)的Zipfian分布

数据集往往在同一元组的属性之间具有高度**相关性(correlation)**。

- 示例:城市的邮政编码，订单日期-发货日期


### Observation

I/O(传统上)是查询执行过程中的主要瓶颈。如果dbms仍然需要读取数据，我们需要确保它可以从中提取最大数量的有用信息。

关键的权衡是速度(speed)与压缩比(compression ratio)

- 压缩数据可以减少对DRAM的需求和处理。

### Today's Agenda

- Background
- Naive Page Compression
- Native Columnar Compression
- Intermediate Data


### Database Compression

减少数据库物理表示(physical representation)的大小，以增加每单位计算或I/O访问和处理的值的数量。

- 目标1:必须生成固定长度的值
- 目标2:必须是无损的方案
- 目标3:理想情况下，延后在查询执行期间尽可能长时间地解压缩时间(?) (只有发现数据匹配, 才会解压缩)


### Database Compression

如果我们想要对dbms进行压缩，我们必须问自己的第一个问题是我们想要压缩的是什么。

这决定了我们可以使用什么压缩方案


### Compression Granularity(粒度)

选择1:block-level

- 压缩块(例如:数据库页，RowGroup)的元组在一个表

选择2: tuple-level

- 压缩整个元组的内容(仅限nsm)

选择#3: attribute-level

- 压缩一个元组中的单个属性值。
- 可以针对同一元组的多个属性。

选择4: column-level

- 压缩为多个元组存储的一个或多个属性的多个值(仅限dsm)

### Naive Compression(朴素压缩)

使用通用算法压缩数据。

压缩范围仅基于作为输入提供的数据。

- LZO(1996)， LZ4(2011)， Snappy(2011)， Brotli(2013)， Oracle OZIP(2014)， Zstd(2015)

注意事项

- 计算开销
- 压缩与解压速度


### Mysql Innodb Compression

disk pages
在每一页的开头会有一个固定长度的mod log
在其中对页面进行更新或者插入
不用先解压缩


### Naive Compression

dbms必须先对数据进行解压缩，然后才能读取和(可能)修改数据。

- 即使算法使用字典压缩，dbms也无法访问字典的内容
- 这限制了压缩方案的实际范围

这些模式也没有考虑数据的high-level的含义或语义。


### Columnar Compression

- Run-length Encoding
- Dictionary Encoding
- Bitmap Encoding
- Delta Encoding
- Bit Packing


### Run-length Encoding

将单个列中的相同值压缩为三元组:
- 属性值
 - 列段的起始位置
- 运行中元素的个数

要求对列进行智能排序，以最大化压缩机会。

如果DBMS只跟踪空白空间，有时也称为空抑制(null suppression)。


```
original data
id  | 1 | 2 | 3 | 4 | 5 |
lit | Y | Y | Y | N | Y |

compressed data
id  | 1       | 2       | 3       |
lit | (Y,0,3) | (N,3,1) | (Y,4,1) |
三元组: (value, offset, length)


sorted data
id  | 1 | 2 | 3 | 5 | 4 |
lit | Y | Y | Y | Y | N |

compressed data
id  | 1       | 2       |
lit | (Y,0,4) | (N,5,1) |
```

### Dictionary Compression

用较小的固定长度代码替换频繁值，然后维护从代码到原始值的映射(字典)

- 通常，每个属性值一个代码
- 在DBMS中使用最广泛的native压缩方案

理想的字典方案支持点查询和范围查询的快速编码和解码


### Dictonary Construction

选择1:一次性完成 (我们的重点)

- 计算给定时间点所有元组的字典。
- 新元组必须使用单独的字典，否则必须重新计算所有元组
- 如果文件是不可变的，这很容易做到

选择2:增量

- 合并新的元组与现有的字典
- 可能需要重新编码到现有的元组


### Dictionary Scope

选择1: block-level

- 在单个表中只包含元组的子集
- DBMS在组合来自不同块的元组时必须解压数据(例如:用于连接的哈希表)

选择2: table-level

- 为整个表构造一个字典
- 更好的压缩比，但昂贵的更新

选择#3: multi-table

- 可以是子集或整个表
- 有时有助于连接和设置操作

### Order-preserving Encoding

编码后的值需要支持与原始值相同的排序顺序。

```
original data
name | Andrea | Wan | Andy | Matt |

compressed data
name | 10     | 40  | 20   | 30   |

value| Andrea | Andy| Matt | Wan  |
code | 10     | 20  | 30   | 40   |

SELECT * FROM users WHERE name LIKE 'And%';
==>
SELECT * FROM users WHERE name BETWEEN 10 AND 20;

但如果是这样的sql:
SELECT name FROM users WHERE name LIKE 'And%';
==> 仍然需要顺序扫描

SELECT DISTINCT name FROM users WHERE name LIKE 'And%';
==> 只需要访问字典
```


### Dictionary Data Structures

选择1:数组

- 一个可变长度字符串数组和另一个带有映射到字符串偏移量指针的数组
- 昂贵的更新，所以只能在不可变的文件中使用

选择2:哈希表

- 快速且紧凑
- 不支持范围和前缀查询

选择3:B+树

- 比哈希表慢，占用更多内存
- 可以支持范围和前缀查询
