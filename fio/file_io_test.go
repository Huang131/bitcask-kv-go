package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIOManager(t *testing.T) {
	// 使用 t.TempDir() 它会自动创建并清理临时目录
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "a.data")
	fio, err := NewFileIOManager(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "a.data")
	fio, err := NewFileIOManager(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("1234567"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "a.data")
	fio, err := NewFileIOManager(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)
	// key-akey-b
	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "a.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	defer fio.Close()

	// 1. 写入数据
	testData := []byte("hello sync test")
	_, err = fio.Write(testData)
	assert.Nil(t, err)

	// 2. 调用 Sync 将数据刷到磁盘
	err = fio.Sync()
	assert.Nil(t, err)

	// 3. 使用标准库重新读取文件，验证数据是否已经成功持久化
	persistedData, err := os.ReadFile(path)
	assert.Nil(t, err)
	assert.Equal(t, testData, persistedData)
}

func TestFileIO_Close(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "a.data")
	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
