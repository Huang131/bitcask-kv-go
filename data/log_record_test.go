package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试编码和解码函数是否能正确地进行往返操作
func TestEncodeDecodeLogRecord(t *testing.T) {
	// 定义一个辅助函数来执行通用的测试逻辑
	testRoundTrip := func(t *testing.T, rec *LogRecord) {
		// 1. 编码 LogRecord
		encBuf, encSize := EncodeLogRecord(rec)
		assert.NotNil(t, encBuf)
		assert.Greater(t, encSize, int64(5))

		// 2. 解码头部
		header, headerSize := decodeLogRecordHeader(encBuf)
		assert.NotNil(t, header)
		assert.Greater(t, headerSize, int64(0))

		// 3. 验证头部信息是否匹配
		assert.Equal(t, rec.Type, header.recordType)
		assert.Equal(t, int64(len(rec.Key)), header.keySize)
		assert.Equal(t, int64(len(rec.Value)), header.valueSize)

		// 4. 验证 CRC 校验和是否正确
		// getLogRecordCRC 需要 header 字节数组（不含 crc 本身）
		crc := getLogRecordCRC(rec, encBuf[crc32.Size:headerSize])
		assert.Equal(t, header.crc, crc)

		// 5. 验证总长度是否一致
		assert.Equal(t, encSize, headerSize+header.keySize+header.valueSize)
	}

	t.Run("normal record", func(t *testing.T) {
		rec := &LogRecord{
			Key:   []byte("name"),
			Value: []byte("bitcask-go"),
			Type:  LogRecordNormal,
		}
		testRoundTrip(t, rec)
	})

	t.Run("record with empty value", func(t *testing.T) {
		rec := &LogRecord{
			Key:  []byte("name"),
			Type: LogRecordNormal,
		}
		testRoundTrip(t, rec)
	})

	t.Run("deleted record", func(t *testing.T) {
		rec := &LogRecord{
			Key:   []byte("name"),
			Value: []byte("bitcask-go"),
			Type:  LogRecordDeleted,
		}
		testRoundTrip(t, rec)
	})
}
