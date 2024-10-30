package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"sort"
	"strings"
	"sync"
)

// SSTable表，存在磁盘中，一个实例对应一个文件
type SSTable struct {
	// 文件句柄
	f        *os.File
	filePath string
	// 元数据 与SSTable一一对应
	tableMetaInfo MetaInfo
	// 文件稀疏索引列表，用于确定元素位置
	// spareIndex map[string]Position
	// ssTable排序后的key列表，作为索引结构
	sortedString []IndexedPosition

	mutex sync.RWMutex
}

type IndexedPosition struct {
	key      string
	position Position
}

type IndexedPositions []IndexedPosition

func (p IndexedPositions) Len() int {
	return len(p)
}

func (p IndexedPositions) Less(i, j int) bool {
	return strings.Compare(p[i].key, p[j].key) < 0
}

func (p IndexedPositions) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (table *SSTable) Init(filePath string) {
	table.filePath = filePath
	table.mutex = sync.RWMutex{}
}

// 加载文件句柄
func (table *SSTable) loadFileHandle() {
	if table.f == nil {
		// 只读方式打开文件
		file, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalf("error open file %v", err)
		}
		table.f = file
	}
	// 加载元数据和稀疏索引数据
	table.loadMetaInfo()
	table.loadSpareIndex()
}

// 从磁盘文件的末尾解析元数据
func (table *SSTable) loadMetaInfo() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}
	info, _ := f.Stat()
	size := info.Size()
	_, err = f.Seek(size-MetaInfoDataByteSize, 0)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.version)
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataStart)
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataLen)
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexStart)
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexLen)
	fmt.Printf("MetaInfo loaded : version-%v dataStart-%v dataLen-%v indexStart-%v indexLen-%v\n",
		table.tableMetaInfo.version, table.tableMetaInfo.dataStart, table.tableMetaInfo.dataLen,
		table.tableMetaInfo.indexStart, table.tableMetaInfo.indexLen)
}

// 加载稀疏索引
func (table *SSTable) loadSpareIndex() {
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	f := table.f
	_, err := f.Seek(table.tableMetaInfo.indexLen, 0)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}
	_, err = f.Read(bytes)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}

	// 反序列化
	sortedString := make([]IndexedPosition, 0)
	err = json.Unmarshal(bytes, &table.sortedString)
	if err != nil {
		log.Fatalf("error Unmarshal bytes to spareIndex %v", err)
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}

	var p IndexedPositions = sortedString
	sort.Sort(p)
	table.sortedString = p
}

// 二分查找sst去寻找元素
func (table *SSTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	lo, hi := 0, len(table.sortedString)
	for lo <= hi {
		mid := (lo + hi) / 2
		if strings.Compare(table.sortedString[mid].key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if hi < 0 || strings.Compare(table.sortedString[hi].key, key) != 0 {
		return kv.Value{}, kv.KeyNotFound
	}

	if table.sortedString[hi].position.Deleted {
		return kv.Value{}, kv.Deleted
	}

	// 否则从磁盘读取
	position := table.sortedString[hi].position
	bytes := make([]byte, position.Len)
	_, err := table.f.Seek(position.Start, 0)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}
	_, err = table.f.Read(bytes)
	if err != nil {
		log.Fatalf("error read file %v", err)
	}

	value, err = kv.Decode(bytes)
	if err != nil {
		log.Fatalf("error decoding value %v", err)
	}
	return value, kv.Success
}
