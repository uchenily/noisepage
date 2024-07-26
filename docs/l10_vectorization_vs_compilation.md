## Vectorization vs. Compilation

### Observation

向量化可以提高查询性能。
编译也可以提高查询性能。

我们还没有讨论哪种方法更好，在什么条件下更好。

在现有的DBMS上切换是困难的，因此必须尽早做出这个设计决策。


### Vectorwise - Precompiled Primitives

预编译大量对类型化数据执行基本操作的“原语”。

- 对每个原语使用简单的kernel意味着它们更容易矢量化

然后，DBMS执行一个查询计划，在运行时调用这些原语。

- 函数调用在多个元组上平摊

-原语的输出是满足该原语所表示的谓词的元组的偏移量


```
SELECT * FROM foo
 WHERE str_col = 'abc'
   AND int_col = 4;

x str_col = 'abc' and int_col = 4
 ^
 |
foo

==>


vec<offset> sel_eq_str(vec<string> batch, string val) {
    vec<offset> res;
    for (offset i = 0; i < batch.size(); i++) {
        if (batch[i] == val) {
            res.append(i);
        }
    }
    return res;
}

vec<offset> sel_eq_int(vec<int> batch, int val, vec<offset> positions) {
    vec<offset> res;
    for (offset i : positions) {
        if (batch[i] == val) {
            res.append(i);
        }
    }
    return res;
}
```


HyPer - Holistic Query Compilation

使用LLVM toolkit将内存中的查询编译为native code。

组织查询处理，使元组在CPU寄存器中保持尽可能长的时间。

- 自下而上/push-based的查询处理模型

- 不可向量化(如最初描述的那样)

```
SELECT * FROM foo
 WHERE str_col = 'abc'
   AND int_col = 4;

x str_col = 'abc' and int_col = 4
 ^
 |
foo

==>


vec<offset> scan_row(vec<string> str_col, string val0, vec<int> int_col, intval1) {
    vec<offset> res;
    for (offset i = 0; i < str_col.size(); i++) {
        if (str_col[i] == val0 && int_col[i] == val1) {
            res.append(i);
        }
    }
    return res;
}
```

### Vectorization vs. Compilation

测试平台系统来分析向量化执行和查询编译之间的权衡。

在每个系统中实现相同的高级算法，但根据系统架构改变实现细节。

- 示例:哈希join算法相同，但系统使用不同的哈希函数(Murmur2 vs. CRC32x2)


### Implementations

方法1: Tectorwise

- 将operations分解为预编译的原语
- 必须在每一步物化(具体化)原语的输出

方法2: Typer

- push-based的处理模型与JIT编译
- 处理单个元组到整个管道，而不具体化中间结果


### TPC-H Workload

> TPC-H 是用来评估OLAP系统的标准基准
> 下面几个用例能代表实际的使用场景

Q1: Fixed-point arithmetic, 4-group aggregation
Q6: Selective filters. Computationally inexpensive
Q3: Join(build: 147k tuples / probe: 3.2 tuples)
Q9: Join (build: 320k tuples / probe: 1.5m tuples)
Q18: High-cardinality aggregation (1.5m groups)

> [benchbase/src/main/java/com/oltpbenchmark/benchmarks/tpch/procedures at main · cmu-db/benchbase (github.com)](https://github.com/cmu-db/benchbase/tree/main/src/main/java/com/oltpbenchmark/benchmarks/tpch/procedures)



### Main Findings

这两种模式都是高效的，并且实现了大致相同的性能。

- 比row-oriented dbms快100倍!

以数据为中心的查询更适合“计算量大”的查询，并且缓存丢失很少。

矢量化在隐藏缓存缺失延迟方面稍好一些。


### SIMD Performance

在Tectorwise上评估矢量化无分支选择(branchless selection)和哈希探测(hash probe)。

使用AVX-512，因为它包含了使用垂直矢量化更容易实现算法的说明。

- 使用位掩码寄存器进行选择性操作。


### Auto-Vectorization

评估编译器自动向量化Vectorwise原语的能力。

- Targets:GCC v7.2, Clang v5.0, ICC v18

ICC能够使用AVX-512对大多数原语进行矢量化:

- Vectorized: Hashing, Selection, Projection
- Not vectorized: Hash Table Probing, Aggregation


### Parting Thoughts

对于所有查询，Vectorwise和HyPer方法之间没有显著的性能差异。

> Andy: 绝大多数系统会使用矢量化, 如果是从零开始构建数据库系统, 会比编译方式更容易启动和运行

### Next Class

Hash Join Implementations

> Andy: 接下来三节课将介绍join. 今年多了一个部分是多路join, 很少有系统这么做, 但是可能是未来十年的方向
