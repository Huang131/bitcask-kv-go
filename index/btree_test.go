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

	// Put a nil key should fail
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.False(t, res1)

	// Put an empty key should fail
	res2 := bt.Put([]byte(""), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.False(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res3)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	// Get a nil key should return nil
	pos1 := bt.Get(nil)
	assert.Nil(t, pos1)

	// Put and Get a normal key
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	pos2 := bt.Get([]byte("a"))
	assert.NotNil(t, pos2)
	assert.Equal(t, int64(2), pos2.Offset)

	// Replace and Get
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)
	pos3 := bt.Get([]byte("a"))
	assert.NotNil(t, pos3)
	assert.Equal(t, int64(3), pos3.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	// Delete a nil key should fail
	res1 := bt.Delete(nil)
	assert.False(t, res1)

	// Put and Delete a normal key
	res2 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res2)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)

	// Get the deleted key should return nil
	pos4 := bt.Get([]byte("aaa"))
	assert.Nil(t, pos4)
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

func TestBTree_Iterator(t *testing.T) {
	t.Run("Empty BTree", func(t *testing.T) {
		bt := NewBTree()
		iter := bt.Iterator(false)
		assert.False(t, iter.Valid())
	})

	bt := NewBTree()
	bt.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})

	t.Run("Forward Iteration", func(t *testing.T) {
		iter := bt.Iterator(false)
		defer iter.Close()
		expectedKeys := [][]byte{[]byte("acee"), []byte("bbcd"), []byte("ccde"), []byte("eede")}
		var actualKeys [][]byte
		for iter.Rewind(); iter.Valid(); iter.Next() {
			actualKeys = append(actualKeys, iter.Key())
		}
		assert.Equal(t, expectedKeys, actualKeys)
	})

	t.Run("Reverse Iteration", func(t *testing.T) {
		iter := bt.Iterator(true)
		defer iter.Close()
		expectedKeys := [][]byte{[]byte("eede"), []byte("ccde"), []byte("bbcd"), []byte("acee")}
		var actualKeys [][]byte
		for iter.Rewind(); iter.Valid(); iter.Next() {
			actualKeys = append(actualKeys, iter.Key())
		}
		assert.Equal(t, expectedKeys, actualKeys)
	})

	t.Run("Forward Seek", func(t *testing.T) {
		iter := bt.Iterator(false)
		defer iter.Close()
		// Seek for "cc", should find "ccde"
		iter.Seek([]byte("cc"))
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("ccde"), iter.Key())

		// Continue iteration
		iter.Next()
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("eede"), iter.Key())
	})

	t.Run("Reverse Seek", func(t *testing.T) {
		iter := bt.Iterator(true)
		defer iter.Close()
		// Seek for "cc", should find "bbcd" in reverse
		iter.Seek([]byte("cc"))
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("bbcd"), iter.Key())

		// Seek for "zz", should find "eede"
		iter.Seek([]byte("zz"))
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("eede"), iter.Key())

		// Continue iteration
		iter.Next()
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("ccde"), iter.Key())
	})
}
