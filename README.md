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
BenchmarkPutGet/put4K-8            2533452      26258 ns/op     6134 B/op       21 allocs/op
BenchmarkPutGet/put4K-8            2973529      27119 ns/op     6204 B/op       21 allocs/op
BenchmarkPutGet/put4K-8            2847762      28744 ns/op     6144 B/op       21 allocs/op
BenchmarkPutGet/batchPut4K-8       4454523      17209 ns/op     1358 B/op       11 allocs/op
BenchmarkPutGet/batchPut4K-8       5207341      17866 ns/op     1354 B/op       11 allocs/op
BenchmarkPutGet/batchPut4K-8       5636701      17583 ns/op     1374 B/op       11 allocs/op
BenchmarkPutGet/get4K-8            5846139      14605 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/get4K-8            5375107      12702 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/get4K-8            5688955      14679 ns/op     9966 B/op       10 allocs/op
BenchmarkPutGet/concurrentGet4K-8  5683243      11450 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentGet4K-8  9601530       9038 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentGet4K-8  6073081      11389 ns/op     9924 B/op        7 allocs/op
BenchmarkPutGet/concurrentPut4K-8  3114558      27029 ns/op     5617 B/op       19 allocs/op
BenchmarkPutGet/concurrentPut4K-8  3332712      29831 ns/op     5729 B/op       19 allocs/op
BenchmarkPutGet/concurrentPut4K-8  2990941      24447 ns/op     5668 B/op       19 allocs/op
PASS
ok   github.com/wenzhang-dev/bitcaskDB/bench 1781.076s
```

The benchmarks with specified disk capacity: [benchmark2](https://github.com/wenzhang-dev/bitcaskDB/blob/main/bench/benchmark2)
