package bitcask_kv_go

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// initDB 是一个测试辅助函数，用于初始化一个 DB 实例以供测试
func initDB(t *testing.T) *DB {
	t.Helper()
	opts := DefaultOptions
	dir := t.TempDir() // 使用 t.TempDir() 自动创建和清理临时目录
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	return db
}

func TestDB_ConcurrentPutGetDelete(t *testing.T) {
	t.Parallel()
	db := initDB(t)
	defer db.Close()

	const numGoroutines = 50
	const numKeysPerGoroutine = 100
	wg := &sync.WaitGroup{}

	// --- Phase 1: Concurrent Puts ---
	t.Run("ConcurrentPut", func(t *testing.T) {
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(gID int) {
				defer wg.Done()
				for j := 0; j < numKeysPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("key-%d-%d", gID, j))
					value := []byte(fmt.Sprintf("value-%d-%d", gID, j))
					err := db.Put(key, value)
					assert.Nil(t, err)
				}
			}(i)
		}
		wg.Wait()
	})

	// --- Phase 2: Concurrent Get/Update/Delete ---
	t.Run("ConcurrentGetUpdateDelete", func(t *testing.T) {
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(gID int) {
				defer wg.Done()
				for j := 0; j < numKeysPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("key-%d-%d", gID, j))

					// 2a. Get and verify original value
					// 在这个并发阶段，我们只关心 Get 操作是否会产生意外错误或数据竞争。
					// key 此时可能已被更新或删除，所以我们不对 value 做精确断言。
					_, err := db.Get(key)
					assert.True(t, err == nil || err == ErrKeyNotFound, "get should not return other errors")

					// 2b. Update some keys (e.g., even keys)
					if j%2 == 0 {
						newValue := []byte(fmt.Sprintf("new-value-%d-%d", gID, j))
						err := db.Put(key, newValue)
						assert.Nil(t, err)
					}

					// 2c. Delete some keys (e.g., keys divisible by 3)
					if j%3 == 0 {
						err := db.Delete(key)
						assert.Nil(t, err)
					}
				}
			}(i)
		}
		wg.Wait()
	})

	// --- Phase 3: Final Verification ---
	t.Run("FinalVerification", func(t *testing.T) {
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < numKeysPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", i, j))
				val, err := db.Get(key)

				if j%3 == 0 { // These keys should have been deleted
					assert.Equal(t, ErrKeyNotFound, err)
					assert.Nil(t, val)
				} else if j%2 == 0 { // These keys should have been updated
					assert.Nil(t, err)
					assert.Equal(t, []byte(fmt.Sprintf("new-value-%d-%d", i, j)), val)
				} else { // These keys should be untouched
					assert.Nil(t, err)
					assert.Equal(t, []byte(fmt.Sprintf("value-%d-%d", i, j)), val)
				}
			}
		}
	})
}

func TestDB_CRUD(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	key1 := []byte("key-1")
	val1 := []byte("value-1")

	t.Run("Put", func(t *testing.T) {
		// 正常 Put
		err := db.Put(key1, val1)
		assert.Nil(t, err)

		// 重复 Put
		err = db.Put(key1, []byte("new-value-1"))
		assert.Nil(t, err)

		// Key 为空
		err = db.Put(nil, val1)
		assert.Equal(t, ErrKeyIsEmpty, err)

		// Value 为空
		err = db.Put([]byte("key-2"), nil)
		assert.Nil(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		// 正常 Get
		retVal, err := db.Get(key1)
		assert.Nil(t, err)
		assert.Equal(t, []byte("new-value-1"), retVal)

		// Get 一个空 Value
		retVal2, err := db.Get([]byte("key-2"))
		assert.Nil(t, err)
		assert.Equal(t, 0, len(retVal2))

		// Get 一个不存在的 Key
		_, err = db.Get([]byte("non-existent-key"))
		assert.Equal(t, ErrKeyNotFound, err)
	})

	t.Run("Delete", func(t *testing.T) {
		// 正常 Delete
		err := db.Delete(key1)
		assert.Nil(t, err)

		// 确认已删除
		_, err = db.Get(key1)
		assert.Equal(t, ErrKeyNotFound, err)

		// 删除一个不存在的 Key
		err = db.Delete([]byte("non-existent-key"))
		assert.Nil(t, err)

		// 删除一个空 Key
		err = db.Delete(nil)
		assert.Equal(t, ErrKeyIsEmpty, err)
	})
}

func TestDB_Restart(t *testing.T) {
	opts := DefaultOptions
	dir := t.TempDir()
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	key1 := []byte("key-1")
	val1 := []byte("value-1")
	key2 := []byte("key-2")

	// 写入/删除一些数据
	assert.Nil(t, db.Put(key1, val1))
	assert.Nil(t, db.Put(key2, []byte("value-2")))
	assert.Nil(t, db.Delete(key2))

	// 关闭数据库
	err = db.Close()
	assert.Nil(t, err)

	// 重新打开
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() { _ = db2.Close() }()

	// 校验数据
	retVal1, err := db2.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, val1, retVal1)

	_, err = db2.Get(key2)
	assert.Equal(t, ErrKeyNotFound, err)
}
