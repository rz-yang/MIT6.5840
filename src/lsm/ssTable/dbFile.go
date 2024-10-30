package ssTable

import (
	"encoding/binary"
	"log"
	"os"
)

func (table *SSTable) GetDbSize() int64 {
	info, err := os.Stat(table.filePath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

func (tree *LevelTree) GetLevelSize(level int) int64 {
	var size int64
	node := tree.levelsTail[level]
	for node != nil {
		size += node.table.GetDbSize()
		node = node.prev
	}
	return size
}

// 数据按约定的格式序列化写入磁盘文件
func writeDataToFile(filePath string, dataArea []byte, indexArea []byte, metaInfo MetaInfo) {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("err opening file", err)
	}
	_, err = f.Write(dataArea)
	if err != nil {
		log.Fatal("err writing to file", err)
	}
	_, err = f.Write(indexArea)
	if err != nil {
		log.Fatal("err writing to file", err)
	}
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.version)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.indexLen)
	err = f.Sync()
	if err != nil {
		log.Fatal("err syncing file", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal("err closing file", err)
	}
}
