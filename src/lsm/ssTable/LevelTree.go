package ssTable

import (
	"fmt"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"sync"
)

// SSTable层次树
type LevelTree struct {
	count      []int
	levelsTail []*tableNode

	mutex sync.RWMutex
}

// 每层的链表节点，指向每层的SSTable
type tableNode struct {
	index int
	table *SSTable
	prev  *tableNode
}

// 从所有的SSTable查找数据
func (tree *LevelTree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	// 每一层遍历
	for _, node := range tree.levelsTail {
		for node != nil {
			value, result := node.table.Search(key)
			if result == kv.KeyNotFound {
				node = node.prev
			} else {
				return value, result
			}
		}
	}
	return kv.Value{}, kv.KeyNotFound
}

func (tree *LevelTree) getCount(level int) int {
	count := 0
	ptr := tree.levelsTail[level]
	for ptr != nil {
		count++
		ptr = ptr.prev
	}
	return count
}

func (tree *LevelTree) Insert(table *SSTable, level int) int {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	tree.count[level]++
	newNode := &tableNode{
		table: table,
		index: tree.count[level],
		prev:  tree.levelsTail[level],
	}
	tree.levelsTail[level] = newNode
	return newNode.index
}

func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return -1, -1, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
