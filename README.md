# bitcask-kv-go

本仓库是 [Bitcask](https://riak.com/assets/bitcask-intro.pdf) 键值存储模型的一个 Go 语言实现。

Bitcask 是一种基于日志结构（Log-Structured）的哈希表，专为快速的键值数据读写而设计。它的核心思想是将所有写操作都以追加（Append-only）的方式写入文件，从而获得极高的写入性能。

## ✨ 功能特性

-   **高性能写入**: 所有写操作均为顺序追加，避免了随机 I/O。
-   **快速读取**: 所有键的索引都存储在内存中，大部分读操作只需一次磁盘寻道。
-   **原子操作**: `Put` 和 `Delete` 操作通过锁机制保证原子性。
-   **数据持久化**: 支持在每次写入后将数据同步到磁盘。
-   **数据文件轮转**: 当活跃数据文件达到预设阈值时，会自动创建新的活跃文件。
-   **数据库重启**: 能够从磁盘上的数据文件重新加载并构建内存索引。
-   **可插拔索引**: 通过接口抽象，目前支持 B-Tree 索引，未来可扩展支持 ART 等其他索引结构。

## 🚀 快速开始

### 使用示例

```go
package main

import (
	bitcask "bitcask-kv-go"
	"fmt"
)

func main() {
	// 使用默认配置，可以自定义
	opts := bitcask.DefaultOptions
	// 指定数据存储目录
	opts.DirPath = "/tmp/bitcask-data"
	
	// 打开数据库
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 写入数据
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	// 读取数据
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val =", string(val)) // 输出: val = bitcask

	// 删除数据
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
    
    // 再次读取
    _, err = db.Get([]byte("name"))
    fmt.Println(err) // 输出: key not found
}
```

## 🧪 运行测试

本项目包含了丰富的单元测试和并发测试用例。你可以使用 `Makefile` 来方便地运行它们。

```sh
# 运行所有常规测试
make test

# 运行所有测试并开启竞态检测器（推荐）
make test-race
```

## 📜 论文参考

Bitcask: A Log-Structured Hash Table for Fast Key/Value Data