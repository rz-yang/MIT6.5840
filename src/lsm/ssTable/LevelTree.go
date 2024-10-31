package ssTable

import (
	"fmt"
	"log"
	"path/filepath"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"strings"
	"sync"
	"time"
)

// SSTable层次树
// 双向指针，其中1+层从头节点开始不断变大
type LevelTree struct {
	count      []int
	levelsHead []*tableNode
	levelsTail []*tableNode

	mutex sync.RWMutex
}

// 每层的链表节点，指向每层的SSTable
type tableNode struct {
	index int
	table *SSTable
	prev  *tableNode
	next  *tableNode
}

// 从所有的SSTable查找数据
// 查找也可以优化，因为1+层level是有序的，所以可以根据最大最小值进行预筛选
func (tree *LevelTree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	// 每一层遍历
	for levelIndex, node := range tree.levelsTail {
		for node != nil {
			// 非第0层，由于有序，可以先判断最小值跟当前key的差距
			if levelIndex != 0 && strings.Compare(key, node.table.GetMinKey()) < 0 {
				node = node.prev
				continue
			}
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
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	return tree.count[level]
}

// Insert 在链表末尾插入
func (tree *LevelTree) Insert(table *SSTable, level int) int {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	tree.count[level]++
	newNode := &tableNode{
		table: table,
		index: tree.count[level],
		prev:  tree.levelsTail[level],
	}
	if tree.levelsHead[level] == nil {
		tree.levelsHead[level] = newNode
	} else {
		tree.levelsTail[level].next = newNode
	}
	tree.levelsTail[level] = newNode
	return newNode.index
}

// 加载db文件到LevelTree，生成索引
func (tree *LevelTree) loadDbFile(path string) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	log.Println("Load levelTree from", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Printf("Load the %v, time consuming %v", path, elapse)
	}()

	level, index, err := getLevel(filepath.Base(path))
	if err != nil {
		log.Println(err)
		return
	}
	table := &SSTable{}
	table.Init(path)
	newNode := &tableNode{
		index: index,
		table: table,
	}
	currentNode := tree.levelsTail[level]
	if currentNode == nil {
		tree.levelsHead[level] = newNode
		tree.levelsTail[level] = newNode
		return
	}
	if currentNode.index < index {
		newNode.prev = currentNode
		currentNode.next = newNode
		tree.levelsTail[level] = newNode
		return
	}

	for currentNode != nil {
		if currentNode.prev == nil || currentNode.prev.index < index {
			if currentNode.prev == nil {
				tree.levelsHead[level] = newNode
			} else {
				currentNode.prev.next = newNode
			}
			newNode.prev = currentNode.prev
			newNode.next = currentNode
			currentNode.prev = newNode
			return
		} else {
			currentNode = currentNode.prev
		}
	}
}

func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return -1, -1, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
