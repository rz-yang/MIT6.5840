package lsm

import (
	"raft_LSMTree-based_KVStore/lsm/kv"
	"sync"
)

type ReadOnlyMemTables struct {
	IMemTables []*IMemTable
	mutex      sync.RWMutex
}

type IMemTable MemTable

func NewReadOnlyMemTables() *ReadOnlyMemTables {
	return &ReadOnlyMemTables{
		IMemTables: make([]*IMemTable, 0),
		mutex:      sync.RWMutex{},
	}
}

func (rt *ReadOnlyMemTables) GetLen() int {
	rt.mutex.RLock()
	defer rt.mutex.RUnlock()
	return len(rt.IMemTables)
}

func (rt *ReadOnlyMemTables) AddTable(table *IMemTable) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()
	rt.IMemTables = append(rt.IMemTables, table)
}

func (rt *ReadOnlyMemTables) GetTable() *IMemTable {
	rt.mutex.RLock()
	defer rt.mutex.RUnlock()
	table := rt.IMemTables[0]
	rt.IMemTables = rt.IMemTables[1:]
	return table
}

func (rt *ReadOnlyMemTables) Search(key string) (kv.Value, kv.SearchResult) {
	rt.mutex.RLock()
	defer rt.mutex.RUnlock()
	for i := len(rt.IMemTables) - 1; i >= 0; i-- {
		value, result := rt.IMemTables[i].MemoryList.Search(key)
		if result == kv.Success {
			kvValue := value.(kv.Value)
			return kvValue, result
		}
	}
	var nilV kv.Value
	return nilV, kv.KeyNotFound
}
