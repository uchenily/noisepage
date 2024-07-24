## Query Complication & Code Generation


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
