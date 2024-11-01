package lsm

import (
	"log"
	"raft_LSMTree-based_KVStore/lsm/config"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"time"
)

func Check() {
	globalConfig := config.GetConfig()
	ticker := time.Tick(time.Duration(globalConfig.CheckInterval) * time.Second)
	for range ticker {
		log.Println("Performing background checks...")
		// 检查是否需要压缩sstable
		database.LevelTree.Check()
	}
}

func checkMemory() {
	globalConfig := config.GetConfig()
	count := database.MemTable.MemoryList.Len()
	if count >= globalConfig.Threshold {
		log.Println("convert memTable to immutable memTable")
		database.Swap()
	}
}

func CompressMemory() {
	globalConfig := config.GetConfig()
	ticker := time.Tick(time.Duration(globalConfig.CheckInterval) * time.Second)
	for range ticker {
		for database.IMemTable.GetLen() != 0 {
			log.Println("Compressing iMemTable")
			preTable := database.IMemTable.GetTable()
			val := preTable.MemoryList.GetValues()
			kvVal := make([]kv.Value, 0, len(val))
			for _, v := range val {
				trueV, ok := v.(kv.Value)
				if !ok {
					log.Println("error converting in CompressMemory")
				}
				kvVal = append(kvVal, trueV)
			}
			database.LevelTree.CreateNewTable(kvVal)
			preTable.Wal.DeleteFile()
		}
	}

}
