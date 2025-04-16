# bitcaskDB 是什么？

bitcaskDB是一个基于bitcask存储模型的轻量级、快速、固定容量的键值对存储引擎。


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

```
go test -bench=PutGet -benchtime=60s -count=3 -timeout=30m
goos: linux
goarch: 386
pkg: github.com/wenzhang-dev/bitcaskDB/bench
cpu: Intel(R) Xeon(R) Gold 5318N CPU @ 2.10GHz
BenchmarkPutGet/put4K-8         	  1638007	     59850 ns/op	   15830 B/op	      29 allocs/op
BenchmarkPutGet/put4K-8         	    1267512	     55906 ns/op	   15506 B/op	      30 allocs/op
BenchmarkPutGet/put4K-8         	   1292365	     57255 ns/op	   15879 B/op	      30 allocs/op
BenchmarkPutGet/batchPut4K-8    	   1843651	     35912 ns/op	   10714 B/op	      17 allocs/op
BenchmarkPutGet/batchPut4K-8    	   2196770	     41649 ns/op	   10367 B/op	      17 allocs/op
BenchmarkPutGet/batchPut4K-8    	   1830928	     44190 ns/op	   10634 B/op	      17 allocs/op
BenchmarkPutGet/get4K-8         	   3837634	     21962 ns/op	    9967 B/op	      10 allocs/op
BenchmarkPutGet/get4K-8         	  3100455	     20038 ns/op	    9967 B/op	      10 allocs/op
BenchmarkPutGet/get4K-8         	   3805360	     20987 ns/op	    9967 B/op	      10 allocs/op
BenchmarkPutGet/concurrentGet4K-8    5285563	     13552 ns/op      10229 B/op	      11 allocs/op
BenchmarkPutGet/concurrentGet4K-8    4609334	     13143 ns/op       9967 B/op	      10 allocs/op
BenchmarkPutGet/concurrentGet4K-8     4857705	     14518 ns/op       9967 B/op	      10 allocs/op
BenchmarkPutGet/concurrentPut4K-8     1266792	     60075 ns/op      15856 B/op	      28 allocs/op
BenchmarkPutGet/concurrentPut4K-8     1657989	     51796 ns/op      14905 B/op	      27 allocs/op
BenchmarkPutGet/concurrentPut4K-8      1305724	     57538 ns/op      15566 B/op	      28 allocs/op
```
