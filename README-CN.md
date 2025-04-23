# bitcaskDB 是什么？

bitcaskDB是一个基于bitcask存储模型的轻量级、快速、固定容量的键值对存储引擎。

它最大的特点是在内存中缓存键值对的索引，每次查询只需要单次 disk seek。按照 100 字节 key，4KB value 的小对象计算，缓存 10 million 个对象，大约需要 1GB 内存，40GB 磁盘空间。相反，如果采用类似 redis，memcached 全内存的缓存方案，相比之下，内存的开销很大。

# 动机

- 硬件资源受限，如 4C8G 100G 磁盘
- 缓存数以千万的小对象


# 特性

- 追加写
- 固定长度的 namespace
- 固定磁盘容量和内存用量
- 细粒度的合并
- 近似 LRU 淘汰策略
- 自定义记录的元数据
- 自定义合并策略
- 自定义挑选策略
- 批量写
- 允许过期时间和数据指纹 Etag
- 基于 hint 的快速恢复
- 软删除

# 对比分析

## LSM
- 追加写
- 读操作可能需要多次随机寻址
- 写放大
  - 链式合并
- 范围查询
- 有序性
- 回收磁盘空间较慢
  - 多个数据版本


## B+Tree
- 原地更新
- 有序性
- 范围查询
- 很难回收磁盘空间


## Bitcask
- 追加写
- 明确的查询和插入性能
- 查询仅需要单次寻址
- 快速的回收磁盘空间
  - 内存仅保留最新的数据版本
- 内存可使用多种数据模型，如 btree，hashtable
  - hashtable 更加紧凑，但无序，不支持范围查询
  - btree 支持范围查询，顺序迭代，但内存开销更大


# 快速开始


```golang
import "github.com/wenzhang-dev/bitcaskDB"

const data = `
<!DOCTYPE html>
<html>
<head>
    <title>Hello Page</title>
</head>
<body>
    <h1>Hello, BitcaskDB!</h1>
</body>
</html>
`

func main() {
    opts := &bitcask.Options{
        Dir:                       "./bitcaskDB",
        WalMaxSize:                1024 * 1024 * 1024, // 1GB
        ManifestMaxSize:           1024 * 1024, // 1MB
        IndexCapacity:             10000000, // 10 million
        IndexLimited:              8000000,
        IndexEvictionPoolCapacity: 32,
        IndexSampleKeys:           5,
        DiskUsageLimited:          1024 * 1024 * 1024 * 100, // 100GB
        NsSize:                    DefaultNsSize,
        EtagSize:                  DefaultEtagSize,
    }

    db, err := bitcask.NewDB(opts)
    if err != nil {
        panic(err)
    }
    defer func() {
        _ = db.Close()
    }()

    ns := GenSha1NS("ns") // fixed-size ns
    key := []byte("testKey")
    value := []byte(data)
    now := uint64(db.WallTime().Unix())

    // customized metadata
    appMeta := make(map[string]string)
    appMeta["type"] = "html"
    meta := NewMeta(appMeta).SetExpire(now+60).SetEtag(GenSha1Etag(value))

    // set a key
    err = db.Put(ns, key, value, meta, &WriteOptions{})
    if err != nil {
        panic(err)
    }

    // get a key
    readVal, readMeta, err := db.Get(ns, key, &ReadOptions{})
    if err != nil {
        panic(err)
    }

    println(readVal)
    println(readMeta)

    // delete a key
    err = db.Delete(ns, key, &WriteOptions{})
    if err != nil {
        panic(err)
    }
}
```

# 性能测试

读写 4KB 的压测报告如下：

```
go test -bench=PutGet -benchtime=60s -count=3 -timeout=30m
goos: linux
goarch: 386
pkg: github.com/wenzhang-dev/bitcaskDB/bench
cpu: Intel(R) Xeon(R) Gold 5318N CPU @ 2.10GHz
BenchmarkPutGet/put4K-8                3368384      29079 ns/op     6162 B/op       20 allocs/op
BenchmarkPutGet/put4K-8                2746623      26084 ns/op     6150 B/op       20 allocs/op
BenchmarkPutGet/put4K-8                3274664      26145 ns/op     6186 B/op       20 allocs/op
BenchmarkPutGet/batchPut4K-8           5798595      17081 ns/op     1359 B/op       11 allocs/op
BenchmarkPutGet/batchPut4K-8           7300834      18230 ns/op     1379 B/op       11 allocs/op
BenchmarkPutGet/batchPut4K-8           5403679      18903 ns/op     1368 B/op       11 allocs/op
BenchmarkPutGet/get4K-8                6199263      14644 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/get4K-8                6532227      13018 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/get4K-8                5358106      13185 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/concurrentGet4K-8      12643495      8655 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentGet4K-8      6455070       9776 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentGet4K-8      11416632      9018 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentPut4K-8      3518620      27436 ns/op     5222 B/op       18 allocs/op
BenchmarkPutGet/concurrentPut4K-8      3096100      24587 ns/op     5164 B/op       18 allocs/op
BenchmarkPutGet/concurrentPut4K-8      3344864      24499 ns/op     5301 B/op       18 allocs/op
BenchmarkPutGet/concurrentBatchPut4K-8 4425745      16762 ns/op     1252 B/op        9 allocs/op
BenchmarkPutGet/concurrentBatchPut4K-8 6332719      16149 ns/op     1279 B/op        9 allocs/op
BenchmarkPutGet/concurrentBatchPut4K-8 5473586      15832 ns/op     1276 B/op        9 allocs/op
PASS
ok   github.com/wenzhang-dev/bitcaskDB/bench 1758.770s
```

同时，也测试了几个主流的 KV 存储引擎在读写 4KB 的性能，并记录了它们在测试过程中的 RSS 占用。
性能测试仓库为：[codebase](https://github.com/wenzhang-dev/bitcaskDB-benchmark)

```shell
go test -bench=Read -benchtime=60s -timeout=30m -count=3
goos: linux
goarch: amd64
pkg: github.com/wenzhang-dev/bitcaskDB-benchmark
cpu: Intel(R) Xeon(R) Gold 5318N CPU @ 2.10GHz
BenchmarkReadWithBitcaskDB/read4K-8  11459024   6313 ns/op  1.217 AvgRSS(GB)  1.275 PeakRSS(GB)  10120 B/op  10 allocs/op
BenchmarkReadWithBitcaskDB/read4K-8  12512324   6522 ns/op  1.220 AvgRSS(GB)  1.234 PeakRSS(GB)  10120 B/op  10 allocs/op
BenchmarkReadWithBitcaskDB/read4K-8  12414660   6468 ns/op  1.206 AvgRSS(GB)  1.231 PeakRSS(GB)  10120 B/op  10 allocs/op
BenchmarkReadWithBadger/read4K-8      4575487  13526 ns/op  2.716 AvgRSS(GB)  4.350 PeakRSS(GB)  19416 B/op  43 allocs/op
BenchmarkReadWithBadger/read4K-8      4960239  13741 ns/op  1.629 AvgRSS(GB)  1.681 PeakRSS(GB)  19406 B/op  43 allocs/op
BenchmarkReadWithBadger/read4K-8      4851144  14429 ns/op  1.591 AvgRSS(GB)  1.650 PeakRSS(GB)  19422 B/op  44 allocs/op
BenchmarkReadWithLevelDB/read4K-8     1569663  50710 ns/op  0.111 AvgRSS(GB)  0.134 PeakRSS(GB)  55021 B/op  35 allocs/op
BenchmarkReadWithLevelDB/read4K-8     1000000  63066 ns/op  0.113 AvgRSS(GB)  0.129 PeakRSS(GB)  54264 B/op  35 allocs/op
BenchmarkReadWithLevelDB/read4K-8     1236408  57268 ns/op  0.114 AvgRSS(GB)  0.138 PeakRSS(GB)  54624 B/op  35 allocs/op
BenchmarkReadWithBoltDB/read4K-8     12587562   5269 ns/op  5.832 AvgRSS(GB)  5.838 PeakRSS(GB)    832 B/op  13 allocs/op
BenchmarkReadWithBoltDB/read4K-8     16920481   4482 ns/op  5.832 AvgRSS(GB)  5.833 PeakRSS(GB)    832 B/op  13 allocs/op
BenchmarkReadWithBoltDB/read4K-8     19141418   5276 ns/op  5.832 AvgRSS(GB)  5.835 PeakRSS(GB)    832 B/op  13 allocs/op
PASS
ok   github.com/wenzhang-dev/bitcaskDB-benchmark 1475.172s
```


```shell
go test -bench=Write -benchtime=60s -timeout=30m -count=3
goos: linux
goarch: amd64
pkg: github.com/wenzhang-dev/bitcaskDB-benchmark
cpu: Intel(R) Xeon(R) Gold 5318N CPU @ 2.10GHz
BenchmarkWriteWithBitcaskDB/write4K-8  8334304  13217 ns/op  0.7905 AvgRSS(GB)   0.934 PeakRSS(GB)    1666 B/op   11 allocs/op
BenchmarkWriteWithBitcaskDB/write4K-8  5323338  14976 ns/op  0.9732 AvgRSS(GB)   1.058 PeakRSS(GB)    1727 B/op   12 allocs/op
BenchmarkWriteWithBitcaskDB/write4K-8  5435398  13929 ns/op  0.9639 AvgRSS(GB)   1.122 PeakRSS(GB)    1756 B/op   12 allocs/op
BenchmarkWriteWithLevelDB/write4K-8    1047753  68691 ns/op  0.0615 AvgRSS(GB)  0.0636 PeakRSS(GB)    2946 B/op   16 allocs/op
BenchmarkWriteWithLevelDB/write4K-8    1179555  71497 ns/op  0.0617 AvgRSS(GB)  0.0634 PeakRSS(GB)    3250 B/op   18 allocs/op
BenchmarkWriteWithLevelDB/write4K-8     992488  74130 ns/op  0.0613 AvgRSS(GB)  0.0625 PeakRSS(GB)    3444 B/op   19 allocs/op
BenchmarkWriteWithBadger/write4K-8     3776720  20036 ns/op   6.409 AvgRSS(GB)   7.534 PeakRSS(GB)   30062 B/op   68 allocs/op
BenchmarkWriteWithBadger/write4K-8     4106070  50959 ns/op   10.77 AvgRSS(GB)   13.63 PeakRSS(GB)  115442 B/op  152 allocs/op
BenchmarkWriteWithBadger/write4K-8     1491906  49955 ns/op   11.45 AvgRSS(GB)   13.72 PeakRSS(GB)   88941 B/op  130 allocs/op
BenchmarkWriteWithBoltDB/write4K-8     2808206  23131 ns/op   0.626 AvgRSS(GB)   0.999 PeakRSS(GB)    7579 B/op   11 allocs/op
BenchmarkWriteWithBoltDB/write4K-8     4303538  22836 ns/op   1.713 AvgRSS(GB)   2.971 PeakRSS(GB)    7765 B/op   11 allocs/op
BenchmarkWriteWithBoltDB/write4K-8     3755002  19385 ns/op   2.481 AvgRSS(GB)   2.872 PeakRSS(GB)    7896 B/op   12 allocs/op
PASS
ok   github.com/wenzhang-dev/bitcaskDB-benchmark 1541.068s
```

指定磁盘容量的压测报告: [benchmark2](https://github.com/wenzhang-dev/bitcaskDB/blob/main/bench/benchmark2)
