package lsm

import (
	"log"
	"os"
	"path"
	"raft_LSMTree-based_KVStore/lsm/ssTable"
	"raft_LSMTree-based_KVStore/lsm/wal"
)

type Database struct {
	// 内存表
	MemTable *MemTable
	// 只读内存表
	IMemTable *ReadOnlyMemTables
	// SSTable
	levelTree *ssTable.LevelTree
}

// 数据库全局唯一实例
var database *Database

func (db *Database) loadAllWalFiles(dir string) {
	infos, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, info := range infos {
		name := info.Name()
		if path.Ext(name) == ".log" {
			preWal := &wal.Wal{}
			preTree := preWal.LoadFromFile(path.Join(dir, name), db.MemTable.MemoryList)
			table := &IMemTable{
				MemoryList: preTree,
				Wal:        preWal,
			}
			log.Printf("add table to iMemTable, table: %v\n", table)
			db.IMemTable.AddTable(table)
		}
	}
}

func (db *Database) Swap() {
	table := db.MemTable.swap()
	// 将内存表存储到IMemTable
	db.IMemTable.AddTable(table)
}
