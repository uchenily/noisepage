## Vectorized Query Execution with SIMD


### Last Class

我们讨论了dbms如何在它的workers之间划分任务来执行查询。

dbms需要知道数据的位置，以避免非本地内存访问。


### Today's Agenda

Backend

Implementation Approaches

SIMD Fundamentals

Vectorized DBMS Algorithms


### Vectorization

将一次处理单个操作数对的算法的标量实现(scalar implementation)转换为一次处理多个操作数对的向量实现(vector implementation)的过程。

### Why This Matters

假设我们可以在32核上并行化我们的算法。假设每个核心都有一个4-wide SIMD寄存器。

潜在加速倍数:32x * 4x = 128x


### SIMD: Single Instruction, Multiple Data

一类cpu指令，它允许处理器同时对多个数据点执行相同的操作。

所有主要ISA的微架构都支持SIMD操作:
- x86: MMX, SSE, SSE2, SSE3, SSE4, AVX, AVX2, AVX512
- PowerPC: Altivec
- ARM: NEON, SVE
- RISC-V: RVV

### SIMD Example

```
X + Y = Z

[x1]      [y1]    [x1 + y1]
[x2]      [y2]    [x2 + y2]
...   +        =  ...

[x_n]     [y_n]   [x_n + y_n]


for (i = 0; i < n; i++) {
    Z[i] = X[i] + Y[i];
}
```

### Vectorization Direction

方法1:水平

- 在单个向量中对所有元素进行操作

```
| 0 | 1 | 2 | 3 |

SIMD Add ==> 6
```

方法2:垂直 (数据库中更常见)

- 以元素方式对每个向量的元素进行操作

```
| 0 | 1 | 2 | 3 |

     SIMD Add ==>  | 1 | 2 | 3 | 4 |

| 1 | 1 | 1 | 1 |
```

### SIMD Instructions

Data Movement
- Moving data in and out of vector registers

Arithmetic Operations
- Apply operation on multiple data items (eg. 2doubles, 4floats, 16bytes)
- Example: ADD, SUB, MUL, DIV, SQRT, MAX, MIN

Logical Instructions
- Logical operations on multiple data items
- Example: AND, OR, XOR, ANDN, ANDPS, ANDNPS

Comparison Instructions
- Comparing multiple data imtes (`==, <, <=, >, >=, !=`)

Shuffle instructions
- Move data between SIMD registers

Miscellaneous
- Conversion: Transform data between x86 and SIMD registers.
- Cache Control: Move data directly from SIMD registers to memory (bypassing CPU cache)


### Intel SIMD Extensions

|      |         | Width   | Integers | Single-P | Double-P |
| ---- | ------- | ------- | -------- | -------- | -------- |
| 1997 | MMX     | 64bits  | Y        |          |          |
| 1999 | SSE     | 128bits | Y        | Y        |          |
| 2001 | SSE2    | 128bits | Y        | Y        | Y        |
| 2004 | SSE3    | 128bits | Y        | Y        | Y        |
| 2006 | SSSE 3  | 128bits | Y        | Y        | Y        |
| 2006 | SSE 4.1 | 128bits | Y        | Y        | Y        |
| 2008 | SSE 4.2 | 128bits | Y        | Y        | Y        |
| 2011 | AVX     | 256bits | Y        | Y        | Y        |
| 2013 | AVX2    | 256bits | Y        | Y        | Y        |
| 2017 | AVX-512 | 512bits | Y        | Y        | Y        |


### ISMD Trade-Offs

优点:

- 算法的显著性能提升和资源利用率可以矢量化。

缺点:

- 使用SIMD实现算法仍然主要是一个手动过程
- SIMD可能对数据对齐有限制
- 将数据收集到SIMD寄存器并将其分散到正确的位置是棘手和/或低效的 (No Longer True in AVX-512!)


### AVX-512

Intel对AVX2指令的512位扩展。

- 提供新的操作，支持数据的转换、分散和排列。(conversions, scatter and permutations)

与以前的SIMD扩展不同，英特尔将AVX-512分成cpu组并有选择地提供(除了“foundation”扩展AVX-512F)。


### Implementation

选择1:自动矢量化

选择2:编译器提示

选择3:显式矢量化

(easy of use --> programmer control), 所有数据库厂商都在使用第三种方式


### Automatic Vectorization

编译器可以识别何时可以将循环中的指令重写为向量化操作。

仅适用于简单循环，在数据库操作符中很少见。需要硬件支持SIMD指令。


```
void add(int *X, int *Y, int *Z) {
    for (int i = 0; i < MAX; i++) {
        Z[i] = X[i] + Y[i];
    }
}

这个循环不能自动向量化, 因为X/Y/Z可能指向相同的地址

代码是按照顺序描述加法的方式编写的
```

### Compiler Hints

向编译器提供有关代码的附加信息，让它知道矢量化是安全的。

两种方法:

- 提供有关内存位置的明确信息
- 告诉编译器忽略向量依赖

```
void add(int *restrict X, int *restrict Y, int *restrict Z) {
    for (int i = 0; i < MAX; i++) {
        Z[i] = X[i] + Y[i];
    }
}

c++中的restrict关键字告诉编译器数组在内存中是不同的位置
```

```
void add(int *X, int *Y, int *Z) {
#pragma ivdep
    for (int i = 0; i < MAX; i++) {
        Z[i] = X[i] + Y[i];
    }
}

这个pragma告诉编译器忽略vector的循环依赖

dbms开发人员需要确保这是正确的
```


### Explicit Vectorization

使用CPU instrinsics在SIMD寄存器之间手动封送数据并执行向量化指令。

-不可跨cpu (isa /版本)移植。

有一些库隐藏了对SIMD intrinsics的底层调用:
- Google Highway
- Simd
- Expressive Vector Engine (EVE)
- std::simd (Experimental)


```
void add(int *X, int *Y, int *Z) {
    __mm128i *vecX = (__m128i*)X;
    __mm128i *vecY = (__m128i*)Y;
    __mm128i *vecZ = (__m128i*)Z;
    for (int i = 0; i < MAX / 4; i++) {
        _mm_store_si128(vecZ++, _mm_add_epi32(*vecX++, *vecY++));
    }
}

将向量存储在128位SIMD寄存器中。

然后调用intrinsic将向量相加并将它们写入输出位置
```

### Vectorization Fundamentals

DBMS将使用一些基本的SIMD操作来构建更复杂的功能:

- 屏蔽(masking)
- 交换(permute)
- 可选择加载/存储(selective load/store)
- 压缩/扩展(compress/expand)
- 选择性收集/分散(selective gather/scatter)

### SIMD Masking

几乎所有AVX-512操作都支持预测变体，即CPU只在输入位掩码指定的通道上执行操作。

```
merge source
| 9 | 9 | 9 | 9 |

vector1
| 3 | 3 | 3 | 3 |

mask
| 0 | 1 | 0 | 1 |       Add    | 9 | 5 | 9 | 5 |

vector2
| 2 | 2 | 2 | 2 |
```

### Permute

对于每个通道，将由索引向量中的偏移量指定的输入向量中的值复制到目标向量中

在AVX-512之前，dbms必须将数据从simd寄存器写入内存，然后再返回到simd寄存器


```
index vector
| 3 | 0 | 3 | 1 |

input vector
| A | B | C | D |
( 0   1   2   3 )


value vector
| D | A | D | B |
```


### Selective Load/Store

```
selective load

vector
| A | B | C | D | ==> | A | U | C | V |

mask
| 0 | 1 | 0 | 1 |

memory
| U | V | W | X | Y | Z | ...
```


```
selective store

memory
| U | V | W | X | Y | Z | ... ==> | B | D | W | X | ...

mask
| 0 | 1 | 0 | 1 |

vector
| A | B | C | D |
```


### Compress / Expand

```
compress

value vector
|   |   |   |   | ==> | A | D | 0 | 0 |

index vector
| 1 | 0 | 0 | 1 |
input vector
| A | B | C | D |
```


```
expand

value vector
|   |   |   |   | ==> | A | B | 0 | 0 |

index vector
| 0 | 1 | 0 | 1 |
input vector
| A | B | C | D |
```

### Selective Scatter/Gather

```
selective gather

value vector
| A | B | C | D | ==> | W | V | Z | X |

index vector
| 2 | 1 | 5 | 3 |

memory
| U | V | W | X | Y | Z | ...
( 0   1   2   3   4   5 )
```

```
selective scatter

memory
| U | V | W | X | Y | Z | ... ==> | U | B | A | D | Y | C | ...
( 0   1   2   3   4   5 )

value vector
| A | B | C | D |

index vector
| 2 | 1 | 5 | 3 |
```


### Vectorized DBMS algorithms

通过使用基本向量操作来构造更高级功能的有效矢量化原理。

- 通过处理每个车道(lane)不同的输入数据来支持垂直矢量化

- 通过执行每个车道(lane)子集的唯一数据项来最大化车道利用率(即，没有无用的计算)

Selection Scans
Hash Tables
Partitioning / Histograms


### Selection Scans

```
SELECT * FROM table WHERE key >= $low AND key <= $high;

# scalar (branchless)
i = 0
for t in table:
    copy(t, output[i])
    key = t.key
    m = (key >= low ? 1 : 0) & (key <= high ? 1 : 0)
    i = i + m


vectorized
i = 0
for vt in table:
    simd_load(vt.key, vk)
    vm = (vk >= low ? 1 : 0) & (vk <= high ? 1 : 0)
    simd_store(vt, vm, output[i])
    i = i + | vm!=false |

SELECT * FROM table WHERE key >= "O" AND key <= "U";
```

```
| id | 1 | 2 | 3 | 4 | 5 | 6 |
| key| J | O | Y | S | U | X |

key vector
| J | O | Y | S | U | X |

simd comparse

==>
mask
| 0 | 1 | 0 | 1 | 1 | 0 |


all offset
| 0 | 1 | 2 | 3 | 4 | 5 |

simd compress
| 1 | 3 | 4 |
```


### Observation

对于每个批处理，SIMD向量可能包含不再有效的元组(它们被先前的一些检查取消了资格)。

```
SELECT COUNT(*) FROM table WHERE age > 20 GROUP BY city;

agg = dict()
for t in table:                    # filter
    if t.age > 20:                 # filter
        agg[t.city]['count']++     # filter
for t in agg:                      # emit
    emit(t)                        # emit

# Scan -> Filter -> Agg -> Emit
# pipeline#1            | pipeline#2
```


### Relaxed Operator Fusion

为查询复杂性执行引擎设计的矢量化处理模型。

将管道分解为对元组的向量进行操作(stages)的阶段

- 每个阶段可以包含多个操作符
- 通过驻留缓存的缓冲区进行通信
- stages是矢量化+融合的粒度

```
SELECT COUNT(*) FROM table WHERE age > 20 GROUP BY city;

# Scan -> Filter -> Agg -> Emit
# pipeline#1            | pipeline#2

==>

# Scan -> Filter [Stage Buffer] -> Agg -> Emit
# stage#1               |         stage#2

agg = dict()
for vt in table step 1024:                # stage#1
    buffer = simd_cmp_gt(vt, 20, 1024)    # stage#1
    if |buffer| >= MAX:
        for t in buffer:                  # stage#2
            agg[t.city]['count']++        # stage#2
for t in agg:                             # emit
    emit(t)                               # emit
```


### For software Prefetching

DBMS可以告诉CPU在处理当前批处理时抓取下一个向量。

- 启用预取的操作符定义新阶段的开始
- 隐藏cache miss时延

任何预取技术都是合适的

- 组预取，软件流水线，AMAC
 - 组预取有效且实现简单


### Hash Tables - Probing


```
scalar
input key             hash(key)            hash index
k1                    #                    h1

key | payload
    |
    |
    |


vectorized(Horizontal)
input key             hash(key)            hash index
k1                    #                    h1

k1 = k9 | k3 | k8 | k1

simd compare

matched mask
| 0 | 0 | 0 | 1 |


(linear probing bucketized hash table)
keys        | payload
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |


vectorized (Vertical)
input key vector            hash(key)           hash index vector
k1                          #                   h1
k2                          #                   h2
k3                          #                   h3
k4                          #                   h4

假设探测结果是这样的:

k1 = k1     => 1
k2 = k99    => 0
k3 = k88    => 0
k4 = k4     => 1

simd compare
```


### Partitioning - Histogram

使用分散和聚集来增加计数。

复制直方图来处理碰撞。


```
input key vector       hash index vector        histogram
k1                     h1
k2                     h2                       +1
k3                     h3                       +1 (两次, 这里会错过更新)
k4                     h4
                                                +1
        simd radix              simd add
```

```
input key vector       hash index vector        replicated histogram     histogram
k1                     h1                       |  |  |  |  |
k2                     h2                       |+1|  |  |  |            +1
k3                     h3                       |  |+1|  |+1|            +2
k4                     h4                       |  |  |  |  |
                                                |  |  |+1|  |            +1
        simd radix              simd scatter    ^^^^^^^^^^^^^
                                                # of vector lanes
```


### Caveat Emptor

AVX-512并不总是比AVX2快

一些cpu在切换到AVX-512模式时会降低其时钟速度。

- 编译器将倾向于256位SIMD操作。

如果只有一小部分进程使用AVX-512，那么就不值得付出downclock的代价。


### Parting Thoughts

向量化对于OLAP查询至关重要。

我们可以在数据库管理系统中结合我们讨论过的所有查询内并行性优化。

- 多个线程处理同一个查询
- 每个线程可以执行一个编译计划
- 编译后的计划可以调用向量化操作

> Andy 说一般有2-4倍的性能提升


### Next Class

Query Compilation
Project #3 Topics
