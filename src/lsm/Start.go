package lsm

import (
	"log"
	"os"
	"raft_LSMTree-based_KVStore/lsm/config"
	"raft_LSMTree-based_KVStore/lsm/ssTable"
)

// 启动数据库
func Start(globalConfig config.Config) {
	log.Println("Loading a Configuration File")
	config.SetConfig(globalConfig)
	log.Println("Initializing the database")
	initDatabase(globalConfig.DataDir)

	log.Println("Performing background checks...")
	checkMemory()
	database.LevelTree.Check()
	go Check()
	go CompressMemory()
}

// 初始化Database
func initDatabase(dir string) {
	database.Init(&MemTable{}, &ReadOnlyMemTables{}, &ssTable.LevelTree{})
	// 从磁盘文件恢复数据
	if _, err := os.Stat(dir); err != nil {
		log.Println("Database directory does not exist")
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}
	database.IMemTable.Init()
	database.MemTable.InitMemList()
	log.Println("Loading all wal.log...")
	database.loadAllWalFiles(dir)
	database.MemTable.InitWal(dir)
}
