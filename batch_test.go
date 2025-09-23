package bitcask_kv_go

import (
	"bitcask-kv-go/utils"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// initDBForBatch 是一个测试辅助函数，用于初始化一个 DB 实例以供测试
func initDBForBatch(t *testing.T) *DB {
	t.Helper()
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	return db
}

func TestDB_WriteBatch(t *testing.T) {
	t.Run("Basic Put/Delete", func(t *testing.T) {
		db := initDBForBatch(t)
		defer db.Close()

		// 1. 写入数据到批次中，但不提交
		wb := db.NewWriteBatch(DefaultWriteBatchOptions)
		err := wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
		assert.Nil(t, err)
		err = wb.Delete(utils.GetTestKey(2))
		assert.Nil(t, err)

		// 2. 在提交前，数据不应该在数据库中可见
		_, err = db.Get(utils.GetTestKey(1))
		assert.Equal(t, ErrKeyNotFound, err)

		// 3. 正常提交数据
		err = wb.Commit()
		assert.Nil(t, err)

		// 4. 提交后，数据应该可见
		val1, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, val1)
		assert.Nil(t, err)

		// 5. 创建新批次，删除有效的数据
		wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
		err = wb2.Delete(utils.GetTestKey(1))
		assert.Nil(t, err)
		err = wb2.Commit()
		assert.Nil(t, err)

		// 6. 确认数据已被删除
		_, err = db.Get(utils.GetTestKey(1))
		assert.Equal(t, ErrKeyNotFound, err)
	})

	t.Run("Restart Persistence and SeqNo", func(t *testing.T) {
		opts := DefaultOptions
		opts.DirPath = t.TempDir() // 使用 t.TempDir() 自动创建和清理临时目录
		db, err := Open(opts)
		assert.Nil(t, err)

		// 写入一个非事务性数据
		err = db.Put(utils.GetTestKey(0), utils.RandomValue(10))
		assert.Nil(t, err)

		// 提交一个批处理
		wb := db.NewWriteBatch(DefaultWriteBatchOptions)
		err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
		assert.Nil(t, err)
		err = wb.Delete(utils.GetTestKey(0))
		assert.Nil(t, err)
		err = wb.Commit() // seqNo -> 1
		assert.Nil(t, err)

		// 再次提交，测试批次重用
		err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
		assert.Nil(t, err)
		err = wb.Commit() // seqNo -> 2
		assert.Nil(t, err)

		// 重启数据库
		err = db.Close()
		assert.Nil(t, err)

		db2, err := Open(opts)
		assert.Nil(t, err)
		defer db2.Close()

		// 验证数据
		_, err = db2.Get(utils.GetTestKey(0))
		assert.Equal(t, ErrKeyNotFound, err) // 应该被删除了

		// 验证序列号是否正确加载
		assert.Equal(t, uint64(2), db2.seqNo)
	})

	t.Run("Empty Batch", func(t *testing.T) {
		db := initDBForBatch(t)
		defer db.Close()

		wb := db.NewWriteBatch(DefaultWriteBatchOptions)
		err := wb.Commit()
		assert.Nil(t, err, "committing an empty batch should not produce an error")
	})

	t.Run("Exceed MaxBatchNum", func(t *testing.T) {
		db := initDBForBatch(t)
		defer db.Close()

		opts := DefaultWriteBatchOptions
		opts.MaxBatchNum = 1 // 设置一个较小的限制

		wb := db.NewWriteBatch(opts)
		err := wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
		assert.Nil(t, err)
		err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
		assert.Nil(t, err)

		// 提交时应该会因为超出数量而返回错误
		err = wb.Commit()
		assert.Equal(t, ErrExceedMaxBatchNum, err)

		// 验证在提交失败后，批处理中的数据没有丢失
		assert.Equal(t, 2, len(wb.pendingWrites))
	})

	t.Run("Last Write Wins in Batch", func(t *testing.T) {
		db := initDBForBatch(t)
		defer db.Close()

		key1, val1 := utils.GetTestKey(100), utils.RandomValue(10)
		key2, val2 := utils.GetTestKey(101), utils.RandomValue(10)
		key3, val3_old, val3_new := utils.GetTestKey(102), utils.RandomValue(10), utils.RandomValue(10)

		wb := db.NewWriteBatch(DefaultWriteBatchOptions)

		// 场景1: Put -> Put (overwrite)，最终应该是新值
		assert.Nil(t, wb.Put(key3, val3_old))
		assert.Nil(t, wb.Put(key3, val3_new))

		// 场景1: Put -> Delete，最终应该被删除
		assert.Nil(t, wb.Put(key1, val1))
		assert.Nil(t, wb.Delete(key1))

		// 场景2: Delete -> Put，最终应该存在
		assert.Nil(t, wb.Delete(key2)) // 先删除一个不存在的 key
		assert.Nil(t, wb.Put(key2, val2))

		err := wb.Commit()
		assert.Nil(t, err)

		// 验证 key1 确实被删除了
		_, err = db.Get(key1)
		assert.Equal(t, ErrKeyNotFound, err)

		// 验证 key2 存在且值为 val2
		retVal, err := db.Get(key2)
		assert.Nil(t, err)
		assert.Equal(t, val2, retVal)

		// 验证 key3 存在且值为新值
		retVal3, err := db.Get(key3)
		assert.Nil(t, err)
		assert.Equal(t, val3_new, retVal3)
	})

	t.Run("Concurrent Commits", func(t *testing.T) {
		t.Parallel()
		db := initDBForBatch(t)
		defer db.Close()

		const numGoroutines = 10
		const numKeysPerBatch = 10
		wg := &sync.WaitGroup{}
		wg.Add(numGoroutines)

		// 每个 goroutine 创建自己的 WriteBatch 并提交
		for i := 0; i < numGoroutines; i++ {
			go func(gID int) {
				defer wg.Done()
				wb := db.NewWriteBatch(DefaultWriteBatchOptions)
				for j := 0; j < numKeysPerBatch; j++ {
					key := []byte(fmt.Sprintf("key-%d-%d", gID, j))
					val := utils.RandomValue(10)
					err := wb.Put(key, val)
					assert.Nil(t, err)
				}
				err := wb.Commit()
				assert.Nil(t, err)
			}(i)
		}
		wg.Wait()

		// 最终校验所有数据是否都已成功写入
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < numKeysPerBatch; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", i, j))
				_, err := db.Get(key)
				assert.Nil(t, err, "key %s should exist after concurrent commits", key)
			}
		}
	})
}
