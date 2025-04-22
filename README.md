<div align="center">
<strong>
<samp>

[English](https://github.com/wenzhang-dev/bitcaskDB/blob/main/README.md) · [简体中文](https://github.com/wenzhang-dev/bitcaskDB/blob/main/README-CN.md)

</samp>
</strong>
</div>

# What is bitcaskDB

bitcaskDB is a light-weight, fast, fixed capacity key/value storage engine base on bitcask storage model.

Its biggest feature is that it caches the index of key-value pairs in memory, and each query only requires a single disk seek. Based on the calculation of small objects with 100 bytes of key and 4KB of value, caching 10 million objects requires about 1GB of memory and 40GB of disk space. On the contrary, if a full-memory caching solution like redis or memcached is used, the memory overhead is very high.


# Motivation

- limited hardware resources
- cache tens of millions of small objects


# Features
- append-only
- fixed-size namespace
- fixed memory and disk usage
- fine-grained compaction
- LRU-like eviction policy in memory
- customized record metadata
- customized compaction filter
- customized compaction picker
- bulk writes
- allow expire and value fingerprint(etag)
- fast recover based on hint wal
- soft deletion


# Comparation

## LSM
- append-only
- multiple disk seek in worst case
- write amplification
  - chained compaction
- range search
- ordered
- slow to reclaim disk space
  - multiple data version


## B+Tree
- update-inplace
- ordered
- range search
- hard to reclaim disk space


## Bitcask
- append-only
- predictable lookup and insert performance
- single seek to retrieve any value
- fast to reclaim disk space
  - only one data version in memory
- multiple data model in memory, such as btree, hashtable
  - hashtable is more compact, but un-ordered and un-support range search


# Getting started


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

# Benchmark

Here are the benchmarks for reading and writing 4KB:

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

The benchmarks with specified disk capacity: [benchmark2](https://github.com/wenzhang-dev/bitcaskDB/blob/main/bench/benchmark2)
