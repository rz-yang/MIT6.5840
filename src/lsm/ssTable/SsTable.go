package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
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
	spareIndex map[string]Position
	// ssTable排序后的key列表
	sortedString []string

	mutex sync.Mutex
}

func (table *SSTable) Init(filePath string) {
	table.filePath = filePath
	table.mutex = sync.Mutex{}
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
	table.spareIndex = make(map[string]Position)
	err = json.Unmarshal(bytes, &table.spareIndex)
	if err != nil {
		log.Fatalf("error Unmarshal bytes to spareIndex %v", err)
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		log.Fatalf("error seek file %v", err)
	}

	keys := make([]string, 0, len(table.spareIndex))
	for key := range table.spareIndex {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	table.sortedString = keys

}
