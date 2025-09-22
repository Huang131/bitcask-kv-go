package bitcask_kv_go

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator_CRUD(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Create an iterator for an empty database
	iter := db.NewIterator(DefaultIteratorOptions)
	assert.False(t, iter.Valid())
	iter.Close()

	// Put some data
	err := db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	err = db.Put([]byte("key2"), []byte("value2"))
	assert.Nil(t, err)

	// Test Rewind
	iter = db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key1"), iter.Key())
	val, err := iter.Value()
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)

	// Test Next
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key2"), iter.Key())

	iter.Next()
	assert.False(t, iter.Valid())
}

func TestIterator_PrefixScan(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Put data with different prefixes
	assert.Nil(t, db.Put([]byte("a/1"), []byte("val-a1")))
	assert.Nil(t, db.Put([]byte("a/2"), []byte("val-a2")))
	assert.Nil(t, db.Put([]byte("b/1"), []byte("val-b1")))
	assert.Nil(t, db.Put([]byte("b/2"), []byte("val-b2")))
	assert.Nil(t, db.Put([]byte("c/1"), []byte("val-c1")))

	t.Run("Forward Prefix Scan", func(t *testing.T) {
		opts := DefaultIteratorOptions
		opts.Prefix = []byte("b/")
		iter := db.NewIterator(opts)
		defer iter.Close()

		// The iterator should start at the first key with the prefix "b/"
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("b/1"), iter.Key())

		iter.Next()
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("b/2"), iter.Key())

		// Next should move past the prefix and become invalid
		iter.Next()
		assert.False(t, iter.Valid())
	})

	t.Run("Reverse Prefix Scan", func(t *testing.T) {
		opts := DefaultIteratorOptions
		opts.Prefix = []byte("b/")
		opts.Reverse = true
		iter := db.NewIterator(opts)
		defer iter.Close()

		// The iterator should start at the last key with the prefix "b/"
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("b/2"), iter.Key())

		iter.Next()
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("b/1"), iter.Key())

		// Next should move past the prefix and become invalid
		iter.Next()
		assert.False(t, iter.Valid())
	})

	t.Run("Seek with Prefix", func(t *testing.T) {
		opts := DefaultIteratorOptions
		opts.Prefix = []byte("a/")
		iter := db.NewIterator(opts)
		defer iter.Close()

		// Seek to a key within the prefix range
		iter.Seek([]byte("a/2"))
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("a/2"), iter.Key())

		// Seek to a key before the prefix range, should land on the first valid key
		iter.Seek([]byte("a/0"))
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("a/1"), iter.Key())

		// Seek to a key outside the prefix range, should become invalid
		iter.Seek([]byte("c/1"))
		assert.False(t, iter.Valid())
	})

	t.Run("Seek with Reverse Prefix", func(t *testing.T) {
		opts := DefaultIteratorOptions
		opts.Prefix = []byte("b/")
		opts.Reverse = true
		iter := db.NewIterator(opts)
		defer iter.Close()

		// Seek to a key within the prefix range
		iter.Seek([]byte("b/1"))
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte("b/1"), iter.Key())
	})
}
