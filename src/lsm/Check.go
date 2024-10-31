package lsm

import (
	"log"
	"raft_LSMTree-based_KVStore/lsm/config"
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
			database.LevelTree.CreateNewTable(preTable.MemoryList.GetValues())
			preTable.Wal.DeleteFile()
		}
	}

}
