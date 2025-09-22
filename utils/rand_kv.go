package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	mu      sync.Mutex
)

// GetTestKey 获取测试使用的 key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

// RandomValue 生成随机 value，用于测试
func RandomValue(n int) []byte {
	// 标准库 math/rand 的默认实例和由 rand.New 创建的实例都不是并发安全的
	mu.Lock()
	defer mu.Unlock()
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcask-go-value-" + string(b))
}
