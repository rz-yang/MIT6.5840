package lsm

import (
	"log"
	"raft_LSMTree-based_KVStore/lsm/config"
	"time"
)

func Check() {
	config := config.GetConfig()
	ticker := time.Tick(time.Duration(config.CheckInterval) * time.Second)
	for range ticker {
		log.Println("Performing background checks...")
		// 检查是否需要压缩sstable
		database.LevelTree.Check()
	}
}
