package main

import (
	bitcask "bitcask-kv-go"
	"bitcask-kv-go/utils"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	// 指定数据目录为当前目录下的一个文件夹
	opts.DirPath = "bitcask-data"
	// 设置数据文件大小阈值为 0.5MB，以便测试文件轮转
	opts.DataFileSize = 0.5 * 1024 * 1024

	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 写入一个键，它将存在于第一个（最终会成为旧的）数据文件中
	firstKey := []byte("first-key")
	err = db.Put(firstKey, []byte("this is in the first file"))
	if err != nil {
		panic(err)
	}
	fmt.Println("Put 'first-key' successfully.")

	// 循环写入大量数据，以触发数据文件轮转
	fmt.Println("Putting a large amount of data to trigger file rotation...")
	for i := 0; i < 1000; i++ {
		key := utils.GetTestKey(i)
		value := utils.RandomValue(1024) // 1KB value
		err := db.Put(key, value)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Finished putting large amount of data.")

	// 写入一个键，它将存在于新的活跃数据文件中
	lastKey := []byte("last-key")
	err = db.Put(lastKey, []byte("this is in the new active file"))
	if err != nil {
		panic(err)
	}
	fmt.Println("Put 'last-key' successfully.")

	// 从旧的数据文件中读取
	val1, err := db.Get(firstKey)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Get 'first-key' from older file: %s\n", string(val1))

	// 从新的活跃文件中读取
	val2, err := db.Get(lastKey)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Get 'last-key' from active file: %s\n", string(val2))
}
