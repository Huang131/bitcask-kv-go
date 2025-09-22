package bitcask_kv_go

import (
	"bitcask-kv-go/data"
	"bitcask-kv-go/index"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 文件 id，只能在加载索引的时候使用，不能在其他的地方更新和使用
	activeFile *data.DataFile            // 当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(&options); err != nil {
		return nil, err
	}
	// 判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close 关闭数据库实例
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭活跃文件
	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			return err
		}
	}
	// 关闭旧文件
	for fid, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			log.Printf("Failed to close older file %d: %v", fid, err)
		}
	}
	return nil
}

func checkOptions(options *Options) error {
	if options.DirPath == "" {
		return ErrDatabaseDirPathIsEmpty
	}
	if options.DataFileSize <= 0 {
		return ErrDataFileSizeInvalid
	}
	return nil
}

// 写入 Key/Value 数据，key 不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 加锁，保证写入和更新索引的原子性
	db.mu.Lock()
	defer db.mu.Unlock()

	// 追加写入到当前活跃数据文件
	pos, err := db.appendLogRecordLocked(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 从内存索引中获取 LogRecordPos 位置
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	// 判断数据文件是否存在
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断 LogRecord 类型是否为删除类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 1. 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 加锁，保证检查、写入、删除的原子性
	db.mu.Lock()
	defer db.mu.Unlock()

	// 2. 检查 key 是否存在，如果不存在，直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 3. 构造 LogRecord，标记为删除类型
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}

	// 4. 将删除记录追加写入到数据文件
	_, err := db.appendLogRecordLocked(logRecord)
	if err != nil {
		return err
	}
	// 5. 从内存索引中删除 key，并返回结果
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 追加写数据到活跃文件中（内部实现，调用前需要持有锁）
func (db *DB) appendLogRecordLocked(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有的数据持久到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id进行排序
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个，id是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil

}

// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有的文件id，处理文件中的记录
	for _, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}
	}
	return nil
}
