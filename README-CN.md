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
BenchmarkPutGet/put4K-8            3368384      29079 ns/op     6162 B/op       20 allocs/op
BenchmarkPutGet/put4K-8            2746623      26084 ns/op     6150 B/op       20 allocs/op
BenchmarkPutGet/put4K-8            3274664      26145 ns/op     6186 B/op       20 allocs/op
BenchmarkPutGet/batchPut4K-8       5798595      17081 ns/op     1359 B/op       11 allocs/op
BenchmarkPutGet/batchPut4K-8       7300834      18230 ns/op     1379 B/op       11 allocs/op
BenchmarkPutGet/batchPut4K-8       5403679      18903 ns/op     1368 B/op       11 allocs/op
BenchmarkPutGet/get4K-8            6199263      14644 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/get4K-8            6532227      13018 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/get4K-8            5358106      13185 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/concurrentGet4K-8  12643495      8655 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentGet4K-8  6455070       9776 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentGet4K-8  11416632      9018 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentPut4K-8  3518620      27436 ns/op     5222 B/op       18 allocs/op
BenchmarkPutGet/concurrentPut4K-8  3096100      24587 ns/op     5164 B/op       18 allocs/op
BenchmarkPutGet/concurrentPut4K-8  3344864      24499 ns/op     5301 B/op       18 allocs/op
PASS
ok   github.com/wenzhang-dev/bitcaskDB/bench 1758.770s
```

指定磁盘容量的压测报告: [benchmark2](https://github.com/wenzhang-dev/bitcaskDB/blob/main/bench/benchmark2)
