package skipList

import (
	"math/rand"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"strings"
	"sync"
)

type SkipNode struct {
	// key:string
	key string
	// value:any
	value interface{}
	// nxt:
	next []*SkipNode
}

type SkipList struct {
	// 代表Header节点
	SkipNode
	// RWlock
	mutex sync.RWMutex
	// 临时变量
	// update []*SkipNode
	// 最大层高
	maxL int
	// 比例
	skip int
	// 当前层数
	curLevel int
	// 当前节点数
	length int
}

func NewSkipList() *SkipList {
	skipList := &SkipList{
		maxL:     32,
		skip:     2,
		curLevel: 0,
		length:   0,
		SkipNode: SkipNode{},
		mutex:    sync.RWMutex{},
	}
	skipList.SkipNode.next = make([]*SkipNode, skipList.maxL)
	return skipList
}

func (list *SkipList) Search(key string) (interface{}, kv.SearchResult) {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	var head = &list.SkipNode
	var prev = head
	var nxt *SkipNode
	// 从最高层往下遍历
	for i := list.curLevel - 1; i >= 0; i-- {
		// 当前层的下一个节点
		nxt = prev.next[i]
		for nxt != nil && strings.Compare(nxt.key, key) < 0 {
			prev = nxt
			nxt = nxt.next[i]
		}
	}
	if nxt != nil && nxt.key == key {
		return nxt.value, kv.Success
	} else {
		return nil, kv.KeyNotFound
	}
}

func (list *SkipList) randomLevel() int {
	i := 1
	for ; i < list.maxL; i++ {
		if rand.Int()%list.skip != 0 {
			break
		}
	}
	return i
}

func (list *SkipList) Insert(key string, value interface{}) (interface{}, bool) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	var head = &list.SkipNode
	var prev = head
	var nxt *SkipNode
	// 临时变量
	update := make([]*SkipNode, list.maxL)
	// 从最高层往下遍历
	for i := list.curLevel - 1; i >= 0; i-- {
		// 当前层的下一个节点
		nxt = prev.next[i]
		for nxt != nil && strings.Compare(nxt.key, key) < 0 {
			prev = nxt
			nxt = nxt.next[i]
		}
		update[i] = prev
	}

	// 已存在
	if nxt != nil && nxt.key == key {
		lst := nxt.value
		nxt.value = value
		return lst, true
	}

	// 随机生成新节点的层数
	level := list.randomLevel()
	if level > list.curLevel {
		update[list.curLevel] = head
		level = list.curLevel + 1
		list.curLevel = level
	}

	// 申请新的节点
	node := &SkipNode{
		key:   key,
		value: value,
		next:  make([]*SkipNode, level),
	}

	// 调整指针
	for i := 0; i < level; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	list.length++
	return nil, false
}

func (list *SkipList) Delete(key string) (interface{}, bool) {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	var head = &list.SkipNode
	var prev = head
	var nxt *SkipNode
	// 临时变量
	update := make([]*SkipNode, list.maxL)
	// 从最高层往下遍历
	for i := list.curLevel - 1; i >= 0; i-- {
		// 当前层的下一个节点
		nxt = prev.next[i]
		for nxt != nil && strings.Compare(nxt.key, key) < 0 {
			prev = nxt
			nxt = nxt.next[i]
		}
		update[i] = prev
	}

	if nxt == nil || nxt.key != key {
		return nil, false
	}

	node := nxt
	for i, v := range node.next {
		if update[i].next[i] == node {
			update[i].next[i] = v
			// 这一层空了
			if list.SkipNode.next[i] == nil {
				list.curLevel--
			}
		}
	}

	list.length--
	return node.value, true
}

// clear不加锁，内部函数，不暴露
func (list *SkipList) clear() {
	list.maxL = 32
	list.skip = 2
	list.curLevel = 0
	list.length = 0
	list.SkipNode = SkipNode{}
	list.mutex = sync.RWMutex{}
	list.SkipNode.next = make([]*SkipNode, list.maxL)
}

func (list *SkipList) Copy() *SkipList {
	return &SkipList{
		maxL:     list.maxL,
		skip:     list.skip,
		curLevel: list.curLevel,
		length:   list.length,
		SkipNode: list.SkipNode,
		mutex:    sync.RWMutex{},
	}
}

// 进行一次拷贝和数据迁移，然后再进行初始化所有的数据
func (list *SkipList) Swap() *SkipList {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	iMemableList := list.Copy()
	list.clear()
	return iMemableList
}

func (list *SkipList) Len() int {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	return list.length
}
