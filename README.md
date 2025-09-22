# bitcask-kv-go

论文：[A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
本仓库是Bitcask键值存储模型的一个 Go 语言实现。
Bitcask 是一种基于日志结构（Log-Structured）的哈希表，专为快速的键值数据读写而设计。它的核心思想是将所有写操作都以追加（Append-only）的方式写入文件，从而获得极高的写入性能。

## ✨ 功能特性
-   **高性能写入**: 所有写操作均为顺序追加，避免了随机 I/O。
-   **快速读取**: 所有键的索引都存储在内存中，大部分读操作只需一次磁盘寻道。
-   **并发安全**: `Put`, `Get`, `Delete` 等核心操作通过 `sync.RWMutex` 锁机制保证原子性和并发安全。
-   **数据持久化**: 支持在每次写入后将数据同步到磁盘。
-   **数据文件轮转**: 当活跃数据文件达到预设阈值时，会自动创建新的活跃文件。
-   **数据库重启**: 能够从磁盘上的数据文件重新加载并构建内存索引。
-   **键值迭代**:
    -   提供迭代器支持，可正向或反向遍历所有键。
    -   支持 `Rewind` (回到起点) 和 `Seek` (定位到指定键)。
    -   支持按**前缀**扫描 (Prefix Scan)。
-   **数据完整性校验**: 每条数据记录都包含 CRC32 校验和，确保数据在读写过程中的完整性。
-   **可插拔索引**: 通过接口抽象，目前支持 B-Tree 索引，未来可扩展支持 ART 等其他索引结构。


## ⚙️ 设计与实现
### 存储模型
Bitcask 采用日志追加（Append-only）的存储模型。所有写操作（`Put` 和 `Delete`）都会被编码成一条 `LogRecord`，并顺序写入到活跃数据文件中。删除操作实际上是写入一条标记为“已删除”的特殊记录。

### 数据文件
数据库由一个**活跃数据文件**（Active Data File）和任意数量的**旧数据文件**（Older Data Files）组成。
-   所有写操作都只发生在活跃数据文件上。
-   当活跃文件的大小达到阈值 `DataFileSize` 时，它会被关闭并成为一个只读的旧文件，同时系统会创建一个新的活跃文件。
-   读操作可以在所有文件中进行。

### 内存索引
为了实现快速读取，Bitcask 将所有键和其在数据文件中的位置信息（`LogRecordPos`）存储在内存中。本项目通过 `Indexer` 接口实现了可插拔的索引设计，目前默认使用 `B-Tree`。

### 数据记录格式
每条写入数据文件的记录都遵循以下格式：
    +-------------+-------------+-------------+--------------+-------------+--------------+
    | crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
    +-------------+-------------+-------------+--------------+-------------+--------------+
        4字节          1字节        变长（最大5）   变长（最大5）     变长           变长


## 🚀 快速上手

### 1. 安装
```sh
go get github.com/huang-ba/bitcask-kv-go
```

### 2. 基本使用

```go
package main

import (
	bitcask "bitcask-kv-go"
	"fmt"
	"log"
)

func main() {
	// 使用默认配置，可以自定义
	opts := bitcask.DefaultOptions
	// 指定数据存储目录
	opts.DirPath = "bitcask-data"

	// 打开数据库
	db, err := bitcask.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 写入数据
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		log.Fatal(err)
	}

	// 读取数据
	val, err := db.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("val =", string(val)) // 输出: val = bitcask

	// 删除数据
	err = db.Delete([]byte("name"))
	if err != nil {
		log.Fatal(err)
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