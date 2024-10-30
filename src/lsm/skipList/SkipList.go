package skipList

import (
	"math/rand"
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
	}
	// skipList.update = make([]*SkipNode, skipList.maxL)
	skipList.SkipNode.next = make([]*SkipNode, skipList.maxL)
	return skipList
}
func (list *SkipList) Search(key string) interface{} {
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
		return nxt.value
	} else {
		return nil
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

func (list *SkipList) Insert(key string, value interface{}) {
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
		nxt.value = value
		return
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
}

func (list *SkipList) Delete(key string) interface{} {
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
		return nil
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
	return node.value
}
