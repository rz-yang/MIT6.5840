package lsm

import (
	"fmt"
	"log"
	"raft_LSMTree-based_KVStore/lsm/config"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"raft_LSMTree-based_KVStore/lsm/skipList"
	"raft_LSMTree-based_KVStore/lsm/wal"
	"sync"
)

type MemTable struct {
	// 内存表，以跳表实现
	MemoryList *skipList.SkipList
	// WalF 文件句柄
	Wal   *wal.Wal
	mutex sync.RWMutex
}

func (m *MemTable) InitMemList() {
	log.Println("Initializing MemTable SkipList...")
	m.MemoryList = skipList.NewSkipList()
	m.mutex = sync.RWMutex{}
}

func (m *MemTable) InitWal(dir string) {
	log.Println("Initializing MemTable Wal...")
	m.Wal = &wal.Wal{}
	m.Wal.Init(dir)
}

func (m *MemTable) Search(key string) (kv.Value, kv.SearchResult) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	val, res := m.MemoryList.Search(key)
	convertedVal, ok := val.(kv.Value)
	if !ok {
		fmt.Printf("error converting %v to (kv.Value)\n", val)
	}
	return convertedVal, res
}

func (m *MemTable) Set(key string, value []byte) (kv.Value, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	node := kv.Value{Key: key, Value: value, Deleted: false}
	oldValue, hasOld := m.MemoryList.Insert(key, node)
	oldKVValue, ok := oldValue.(kv.Value)
	if !ok {
		fmt.Printf("error converting %v to (kv.Value)\n", oldValue)
	}
	m.Wal.Write(node)
	return oldKVValue, hasOld
}

func (m *MemTable) Delete(key string) (kv.Value, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	oldValue, success := m.MemoryList.Delete(key)
	oldKVValue, ok := oldValue.(kv.Value)
	if !ok {
		fmt.Printf("error converting %v to (kv.Value)\n", oldValue)
	}
	if success {
		m.Wal.Write(kv.Value{Key: key, Value: nil, Deleted: true})
	}
	return oldKVValue, success
}

// 生成IMemTable，并清空MemTable
func (m *MemTable) swap() *IMemTable {
	config := config.GetConfig()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	iList := m.MemoryList.Swap()
	// 创建iMemableTable
	iTable := &IMemTable{
		MemoryList: iList,
		Wal:        m.Wal,
	}
	newWal := &wal.Wal{}
	newWal.Init(config.DataDir)
	m.Wal = newWal
	return iTable
}
