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

	walUid int
}

func (m *MemTable) InitMemList() {
	log.Println("Initializing MemTable SkipList...")
	m.MemoryList = skipList.NewSkipList()
	m.mutex = sync.RWMutex{}
}

func (m *MemTable) InitWal(dir string) {
	if m.Wal != nil {
		return
	}
	log.Println("Initializing MemTable Wal...")
	m.Wal = &wal.Wal{}
	m.Wal.Init(dir, m.walUid)
	m.walUid++
}

func (m *MemTable) GetUid() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.walUid
}

func (m *MemTable) IncreaseUid() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.walUid++
}

func (m *MemTable) Search(key string) (kv.Value, kv.SearchResult) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	val, res := m.MemoryList.Search(key)
	if res != kv.Success {
		return kv.Value{}, res
	}
	convertedVal, ok := val.(kv.Value)
	if !ok {
		fmt.Printf("error converting %v to (kv.Value)\n", val)
	}
	return convertedVal, res
}

func (m *MemTable) Set(key string, value []byte) (kv.Value, bool, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	node := kv.Value{Key: key, Value: value, Deleted: false}
	oldValue, hasOld := m.MemoryList.Insert(key, node)
	var oldKVValue kv.Value
	var ok bool
	if hasOld {
		oldKVValue, ok = oldValue.(kv.Value)
	}
	if hasOld && !ok {
		fmt.Printf("error converting %v to (kv.Value)\n", oldValue)
	}
	m.Wal.Write(node)

	needSwap := false
	globalConfig := config.GetConfig()
	if m.MemoryList.Len() >= globalConfig.Threshold {
		// 此时需要进行swap()操作
		needSwap = true
	}

	return oldKVValue, hasOld, needSwap
}

func (m *MemTable) Delete(key string) (kv.Value, bool, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	data, _ := kv.Convert(kv.Value{Key: key, Value: nil, Deleted: true})
	oldValue, hasOld, needSwap := m.Set(key, data)
	/*oldValue, success := m.MemoryList.Delete(key)
	oldKVValue, ok := oldValue.(kv.Value)
	if !ok {
		fmt.Printf("error converting %v to (kv.Value)\n", oldValue)
	}*/
	m.Wal.Write(kv.Value{Key: key, Value: nil, Deleted: true})
	return oldValue, hasOld, needSwap
}

// 生成IMemTable，并清空MemTable
func (m *MemTable) swap() *IMemTable {
	globalConfig := config.GetConfig()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	iList := m.MemoryList.Swap()
	// 创建iMemableTable
	iTable := &IMemTable{
		MemoryList: iList,
		Wal:        m.Wal,
	}
	newWal := &wal.Wal{}
	newWal.Init(globalConfig.DataDir, m.walUid)
	m.walUid++
	m.Wal = newWal
	return iTable
}

func (m *MemTable) swapWithoutLock() *IMemTable {
	globalConfig := config.GetConfig()
	iList := m.MemoryList.Swap()
	// 创建iMemableTable
	iTable := &IMemTable{
		MemoryList: iList,
		Wal:        m.Wal,
	}
	newWal := &wal.Wal{}
	newWal.Init(globalConfig.DataDir, m.walUid)
	m.walUid++
	m.Wal = newWal
	return iTable
}
