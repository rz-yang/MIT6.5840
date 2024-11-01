package ssTable

import (
	"log"
	"os"
	"raft_LSMTree-based_KVStore/lsm/config"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"strings"
	"time"
)

// Check 检查是否需要压缩数据库文件
func (tree *LevelTree) Check() {
	tree.majorCompaction()
}

// 进行压缩判断
func (tree *LevelTree) majorCompaction() {
	globalConfig := config.GetConfig()
	for levelIndex, _ := range tree.levelsTail {
		tableSize := int(tree.GetLevelSize(levelIndex) / 1024 / 1024)
		// tree.getCount(levelIndex) >= con.PartSize ||
		// 最后一层不做compaction
		if levelIndex < globalConfig.MaxLevel-1 && tableSize > config.GetLevelMaxSize(levelIndex) {
			tree.doMajorCompactionLevel(levelIndex)
		}
	}
}

// 压缩到下一层
func (tree *LevelTree) doMajorCompactionLevel(levelIndex int) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	log.Println("do MajorCompactionLevel:", levelIndex)
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Println("do MajorCompactionLevel:", levelIndex, elapsed)
	}()
	// to do: 实现低内存开销的log compaction
	// 多路归并
	// level 0 sstable之间是无序的
	// level 1+ 是全部有序的
	// 因此可以在level1+的compaction时进行二路归并，在0时进行多路归并
	// size-tried compaction
	globalConfig := config.GetConfig()
	if levelIndex == 0 {
		// 多路归并
		// 可以先取出level0的所有数据，毕竟数据量不大
		beginNode := tree.levelsHead[levelIndex+1]
		level0Values := tree.getLevel0AllData()
		nxtLevelNode := tree.levelsHead[levelIndex+1]
		curP, nxtP := 0, 0
		var nxtValues []kv.Value
		// 取消指针
		tree.levelsHead[levelIndex+1] = nil
		tree.levelsTail[levelIndex+1] = nil
		tree.count[levelIndex+1] = 0
		// 相同的key只取上层的结果
		values := make([]kv.Value, 0, globalConfig.Threshold)
		for curP < len(level0Values) && nxtLevelNode != nil {
			if nxtValues == nil {
				nxtValues = nxtLevelNode.table.GetAllData()
			}
			key1 := level0Values[curP].Key
			key2 := nxtValues[nxtP].Key
			if strings.Compare(key1, key2) == 0 {
				// 同时移动两个指针
				values = append(values, level0Values[curP])
				curP++
				nxtP++
			} else if strings.Compare(key1, key2) < 0 {
				values = append(values, level0Values[curP])
				curP++
			} else {
				values = append(values, nxtValues[nxtP])
				nxtP++
			}
			if nxtP == len(nxtValues) {
				nxtP = 0
				nxtValues = nil
				nxtLevelNode = nxtLevelNode.next
			}
			if len(values) == globalConfig.Threshold {
				tree.createTable(values, levelIndex+1, true)
				values = make([]kv.Value, 0, globalConfig.Threshold)
			}
		}
		for curP < len(level0Values) {
			values = append(values, level0Values[curP])
			curP++
			if len(values) == globalConfig.Threshold {
				tree.createTable(values, levelIndex+1, true)
				values = make([]kv.Value, 0, globalConfig.Threshold)
			}
		}
		for nxtLevelNode != nil {
			if nxtValues == nil {
				nxtValues = nxtLevelNode.table.GetAllData()
			}
			values = append(values, nxtValues[nxtP])
			nxtP++
			if nxtP == len(nxtValues) {
				nxtP = 0
				nxtValues = nil
				nxtLevelNode = nxtLevelNode.next
			}
			if len(values) == globalConfig.Threshold {
				tree.createTable(values, levelIndex+1, true)
				values = make([]kv.Value, 0, globalConfig.Threshold)
			}
		}
		if len(values) > 0 {
			tree.createTable(values, levelIndex+1, true)
			values = nil
		}
		tree.clearLevel(levelIndex)
		ClearWithBeginNode(beginNode)
		tree.renameLevelTmp(levelIndex + 1)
	} else {
		// 二路归并
		// level层与level+1层进行归并
		beginNode := tree.levelsHead[levelIndex+1]
		curLevelNode := tree.levelsHead[levelIndex]
		nxtLevelNode := tree.levelsHead[levelIndex+1]
		var curValues, nxtValues []kv.Value
		curP, nxtP := 0, 0
		// 取消指针
		tree.levelsHead[levelIndex+1] = nil
		tree.levelsTail[levelIndex+1] = nil
		tree.count[levelIndex+1] = 0
		// 相同的key只取上层的结果
		values := make([]kv.Value, 0, globalConfig.Threshold)
		for curLevelNode != nil && nxtLevelNode != nil {
			if curValues == nil {
				curValues = curLevelNode.table.GetAllData()
			}
			if nxtValues == nil {
				nxtValues = nxtLevelNode.table.GetAllData()
			}
			key1 := curValues[curP].Key
			key2 := nxtValues[nxtP].Key
			if strings.Compare(key1, key2) == 0 {
				// 同时移动两个指针
				values = append(values, curValues[curP])
				curP++
				nxtP++
			} else if strings.Compare(key1, key2) < 0 {
				values = append(values, curValues[curP])
				curP++
			} else {
				values = append(values, nxtValues[nxtP])
				nxtP++
			}
			if curP == len(curValues) {
				curP = 0
				curValues = nil
				curLevelNode = curLevelNode.next
			}
			if nxtP == len(nxtValues) {
				nxtP = 0
				nxtValues = nil
				nxtLevelNode = nxtLevelNode.next
			}
			if len(values) == globalConfig.Threshold {
				tree.createTable(values, levelIndex+1, true)
				values = make([]kv.Value, 0, globalConfig.Threshold)
			}
		}
		for curLevelNode != nil {
			if curValues == nil {
				curValues = curLevelNode.table.GetAllData()
			}
			values = append(values, curValues[curP])
			curP++
			if curP == len(curValues) {
				curP = 0
				curValues = nil
				curLevelNode = curLevelNode.next
			}
			if len(values) == globalConfig.Threshold {
				tree.createTable(values, levelIndex+1, true)
				values = make([]kv.Value, 0, globalConfig.Threshold)
			}
		}
		for nxtLevelNode != nil {
			if nxtValues == nil {
				nxtValues = nxtLevelNode.table.GetAllData()
			}
			values = append(values, nxtValues[nxtP])
			nxtP++
			if nxtP == len(nxtValues) {
				nxtP = 0
				nxtValues = nil
				nxtLevelNode = nxtLevelNode.next
			}
			if len(values) == globalConfig.Threshold {
				tree.createTable(values, levelIndex+1, true)
				values = make([]kv.Value, 0, globalConfig.Threshold)
			}
		}
		if len(values) > 0 {
			tree.createTable(values, levelIndex+1, true)
			values = nil
		}
		tree.clearLevel(levelIndex)
		ClearWithBeginNode(beginNode)
		tree.renameLevelTmp(levelIndex + 1)
	}
}

func ClearNode(oldNode *tableNode) {
	err := oldNode.table.f.Close()
	if err != nil {
		log.Panicf("error close file %v", err)
	}
	err = os.Remove(oldNode.table.filePath)
	if err != nil {
		log.Panicf("error remove file %v", err)
	}
	oldNode.table.f = nil
	oldNode.table = nil
}

func ClearWithBeginNode(oldNode *tableNode) {
	for oldNode != nil {
		ClearNode(oldNode)
		oldNode = oldNode.next
	}
}

func (tree *LevelTree) clearLevel(level int) {
	// tree.mutex.Lock()
	// defer tree.mutex.Unlock()
	oldNode := tree.levelsHead[level]
	for oldNode != nil {
		ClearNode(oldNode)
		oldNode = oldNode.next
	}
	tree.levelsHead[level] = nil
	tree.levelsTail[level] = nil
	tree.count[level] = 0
}

// 对这一层的ssTable进行rename操作
func (tree *LevelTree) renameLevelTmp(level int) {
	node := tree.levelsHead[level]
	for node != nil {
		originalName := node.table.filePath
		targetName := originalName[0:strings.LastIndex(originalName, "_tmp")]
		node.table.FileRename(targetName)
		node = node.next
	}
}
