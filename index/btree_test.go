package index

import (
	"bitcask-kv-go/data"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}

// TestBTree_Concurrent 是一个并发测试用例
// 运行此测试的最佳方式是使用 -race 标志: go test -race -run ^TestBTree_Concurrent$
func TestBTree_Concurrent(t *testing.T) {
	t.Parallel() // 允许此测试与其他测试并行运行
	bt := NewBTree()
	wg := new(sync.WaitGroup)
	const n = 2000 // 增加并发操作的数量

	// --- 阶段一：并发写入 ---
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte("key-" + strconv.Itoa(i))
			pos := &data.LogRecordPos{Fid: uint32(i), Offset: int64(i)}
			bt.Put(key, pos)
		}(i)
	}
	wg.Wait() // 等待所有写入操作完成

	// --- 阶段二：并发读取和删除 ---
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte("key-" + strconv.Itoa(i))

			// 尝试读取
			// 在并发读写阶段，我们只调用 Get，不校验其返回值，主要目的是通过 -race 检测数据竞争
			_ = bt.Get(key)

			// 尝试删除 (偶数 key)
			if i%2 == 0 {
				bt.Delete(key)
			}
		}(i)
	}

	wg.Wait() // 等待所有读取和删除操作完成

	// --- 阶段三：最终校验 ---
	for i := 0; i < n; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		pos := bt.Get(key)
		if i%2 == 0 {
			assert.Nil(t, pos, "even key %s should have been deleted", key)
		} else {
			assert.NotNil(t, pos, "odd key %s should still exist", key)
		}
	}
}
