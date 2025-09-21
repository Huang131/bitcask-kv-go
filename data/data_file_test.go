package data

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dirPath := t.TempDir()
	dataFile1, err := OpenDataFile(dirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	defer dataFile1.Close()

	dataFile2, err := OpenDataFile(dirPath, 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
	defer dataFile2.Close()

	// 重复创建同一个文件
	dataFile3, err := OpenDataFile(dirPath, 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
	defer dataFile3.Close()
}

func TestDataFile_Write(t *testing.T) {
	dirPath := t.TempDir()
	dataFile, err := OpenDataFile(dirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer dataFile.Close()

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("ccc"))
	assert.Nil(t, err)

	assert.Equal(t, int64(9), dataFile.WriteOff)
}

func TestDataFile_Close(t *testing.T) {
	dirPath := t.TempDir()
	dataFile, err := OpenDataFile(dirPath, 123)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer dataFile.Close()

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)

	// 尝试再次写入，应该会失败
	err = dataFile.Write([]byte("bbb"))
	t.Log("dataFile closed, err: ", err)
	assert.NotNil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dirPath := t.TempDir()
	dataFile, err := OpenDataFile(dirPath, 456)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer dataFile.Close()

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dirPath := t.TempDir()
	dataFile, err := OpenDataFile(dirPath, 1024)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer dataFile.Close()

	// 1. 写入一条正常的 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask"),
		Type:  LogRecordNormal,
	}
	encRec1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(encRec1)
	assert.Nil(t, err)

	// 2. 从起始位置读取
	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	// 3. 从新的偏移量开始，写入第二条记录
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("new-value"),
		Type:  LogRecordNormal,
	}
	encRec2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(encRec2)
	assert.Nil(t, err)

	// 4. 从 size1 的位置开始读取第二条记录
	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 5. 读取到文件末尾，应该返回 EOF
	_, _, err = dataFile.ReadLogRecord(size1 + size2)
	assert.Equal(t, io.EOF, err)

	// 6. 测试被删除的数据
	rec3 := &LogRecord{Key: []byte("deleted"), Type: LogRecordDeleted}
	encRec3, _ := EncodeLogRecord(rec3)
	err = dataFile.Write(encRec3)
	assert.Nil(t, err)
	readRec3, _, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3.Type, readRec3.Type)
	assert.Equal(t, rec3.Key, readRec3.Key)
	assert.Empty(t, readRec3.Value)
}
