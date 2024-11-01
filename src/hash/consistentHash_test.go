package hash

import (
	"fmt"
	"testing"
)

func Test_Main(t *testing.T) {
	var ring HashRing
	ring.AddNode("Node1")
	ring.AddNode("Node2")
	ring.AddNode("Node3")
	// 测试数据
	keys := []string{"key1231", "key24123", "key53243", "key123124"}
	for _, key := range keys {
		node := ring.GetNode(key)
		fmt.Printf("%s => %s\n", key, node)
	}
}
