package main

import (
	bitcask "bitcask-kv-go"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	// 指定数据目录为当前目录下的一个文件夹
	opts.DirPath = "bitcask-data"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
