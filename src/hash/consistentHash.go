package hash

import (
	"fmt"
	"hash/crc32"
	"sort"
)

type NodeInfo struct {
	NodeName    string
	VirtualName string
	HashCode    int
}

type HashRing []NodeInfo

var defaultVirtualNodeCnt int = 20

func (r HashRing) Len() int {
	return len(r)
}

func (r HashRing) Less(i, j int) bool {
	return r[i].HashCode < r[j].HashCode
}

func (r HashRing) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r *HashRing) AddNode(node string) {
	// 每个节点k个虚拟节点
	for i := 0; i < defaultVirtualNodeCnt; i++ {
		key := fmt.Sprintf("%s:%d", node, i)
		h := int(crc32.ChecksumIEEE([]byte(key)))
		*r = append(*r, NodeInfo{node, key, h})
	}
	sort.Sort(*r)
}

func (r *HashRing) GetNode(key string) string {
	if len(*r) == 0 {
		return ""
	}
	h := int(crc32.ChecksumIEEE([]byte(key)))
	idx := sort.Search(r.Len(), func(i int) bool {
		return (*r)[i].HashCode >= h
	})
	if idx == r.Len() {
		idx = 0
	}
	return (*r)[idx].NodeName
}
