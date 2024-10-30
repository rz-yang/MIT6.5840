package skipList

import (
	"reflect"
	"testing"
)

func Test_Insert_And_Search(t *testing.T) {
	skipList := NewSkipList()
	skipList.Insert("aaa", "bbb")
	val, _ := skipList.Search("aaa")
	if !reflect.DeepEqual("bbb", val) {
		t.Fatal()
	}
}

func Test_Insert_And_Delete(t *testing.T) {
	skipList := NewSkipList()
	skipList.Insert("aaa", "bbb")
	val, _ := skipList.Delete("aaa")
	if !reflect.DeepEqual("bbb", val) {
		t.Fatal()
	}
}
