package bitcask_kv_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrDatabaseDirPathIsEmpty = errors.New("database dir path is empty")
	ErrDataFileSizeInvalid    = errors.New("database data file size must be greater than 0")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
)
