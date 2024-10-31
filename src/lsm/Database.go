package lsm

import (
	"log"
	"os"
	"path"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"raft_LSMTree-based_KVStore/lsm/ssTable"
	"raft_LSMTree-based_KVStore/lsm/wal"
	"sync"
)

type Database struct {
	// 内存表
	MemTable *MemTable
	// 只读内存表
	IMemTable *ReadOnlyMemTables
	// SSTable
	LevelTree *ssTable.LevelTree
	// 加锁
	rwLock sync.RWMutex
}

// 数据库全局唯一实例
var database *Database

func init() {
	database = new(Database)
}

func (db *Database) Init(memTable *MemTable, iMemTables *ReadOnlyMemTables, levelTable *ssTable.LevelTree) {
	db.MemTable = memTable
	db.IMemTable = iMemTables
	db.LevelTree = levelTable
	db.rwLock = sync.RWMutex{}
}

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
	db.rwLock.Lock()
	defer db.rwLock.Unlock()
	table := db.MemTable.swap()
	// 将内存表存储到IMemTable
	db.IMemTable.AddTable(table)
}

func (db *Database) Search(key string) (kv.Value, bool) {
	db.rwLock.RLock()
	defer db.rwLock.RUnlock()
	// 先查内存表
	value, result := db.MemTable.Search(key)
	if result == kv.Success {
		return value, true
	}
	// 再查iMemTable
	value, result = db.IMemTable.Search(key)
	if result == kv.Success {
		return value, true
	}
	// 最后查 ssTable
	if database.LevelTree != nil {
		value, result = database.LevelTree.Search(key)
		if result == kv.Success {
			return value, true
		}
	}
	var nilV kv.Value
	return nilV, false
}

func (db *Database) Set(key string, value []byte) bool {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()
	_, _, needSwap := db.MemTable.Set(key, value)
	if needSwap {
		go db.Swap()
	}
	return true
}

func (db *Database) Delete(key string) bool {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()
	db.MemTable.Delete(key)
	return true
}
