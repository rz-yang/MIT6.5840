package ssTable

import (
	"encoding/json"
	"log"
	"os"
	"raft_LSMTree-based_KVStore/lsm/config"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"sort"
	"strconv"
	"sync"
)

// 在level 0创建SSTable
func (tree *LevelTree) CreateNewTable(values []kv.Value) {
	tree.createTable(values, 0)
}

// 在level层创建SSTable
func (tree *LevelTree) createTable(values []kv.Value, level int) *SSTable {
	keys := make([]string, 0, len(values))
	sortedString := make([]IndexedPosition, 0, len(values))
	// 数据区
	dataArea := make([]byte, 0)

	for _, value := range values {
		// 编码
		data, err := kv.Encode(value)
		if err != nil {
			log.Fatalf("Error encoding value %v: %v", value.Key, err)
		}
		keys = append(keys, value.Key)
		// 文件定位符
		sortedString = append(sortedString, IndexedPosition{
			key: value.Key,
			position: Position{
				Start:   int64(len(dataArea)),
				Len:     int64(len(data)),
				Deleted: value.Deleted,
			},
		})
		dataArea = append(dataArea, data...)
	}
	var ps IndexedPositions = sortedString
	sort.Sort(ps)

	// 生成索引区
	indexArea, err := json.Marshal(sortedString)
	if err != nil {
		log.Fatalf("Error Marshal sortedstring to indexarea %v: %v", sortedString, err)
	}

	// 生成元数据
	metaInfo := MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}

	sstable := &SSTable{
		tableMetaInfo: metaInfo,
		sortedString:  sortedString,
		mutex:         sync.RWMutex{},
	}
	index := tree.Insert(sstable, level)
	log.Printf("Create a new SSTable, level: %d, index : %d\r\n", level, index)
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	sstable.filePath = filePath

	writeDataToFile(filePath, dataArea, indexArea, metaInfo)
	// 只读方式打开文件
	f, err := os.OpenFile(sstable.filePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatalf("Error opening file %v: %v", filePath, err)
	}
	sstable.f = f
	return sstable
}