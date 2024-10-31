package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path"
	"raft_LSMTree-based_KVStore/lsm/kv"
	"raft_LSMTree-based_KVStore/lsm/skipList"
	"sync"
	"time"
)

type Wal struct {
	f    *os.File
	path string
	lock sync.Mutex
}

func (w *Wal) Init(dir string) {
	log.Println("Loading wal.log...")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Load wal.log took: %s", elapsed)
	}()
	uuidStr := time.Now().Format("20060102_150405")
	walPath := path.Join(dir, uuidStr+"_wal.log")
	log.Printf("init wal.log: %s", walPath)
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error creating wal.log file: %v", err)
	}
	w.f = f
	w.path = walPath
	w.lock = sync.Mutex{}
}

func (w *Wal) LoadFromFile(path string) *skipList.SkipList {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening wal.log file: %v", err)
	}
	w.f = f
	w.path = path
	w.lock = sync.Mutex{}
	return w.LoadToMemory()
}

// 利用wal构建一个完整的SkipList
func (w *Wal) LoadToMemory() *skipList.SkipList {
	w.lock.Lock()
	defer w.lock.Unlock()

	preList := skipList.NewSkipList()
	info, _ := os.Stat(w.path)
	size := info.Size()

	if size == 0 {
		return preList
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Fatalf("error seeking to beginning of file: %v", err)
	}
	// 将指针移到结尾，方便append操作
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Fatalf("error seeking to beginning of file: %v", err)
		}
	}(w.f, size-1, 0)

	// 读到用户态缓存
	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Fatalf("error reading wal.log file: %v", err)
	}

	dataLen := int64(0)
	index := int64(0)
	for index < size {
		// 前8个字节代表长度（uint32）
		indexData := data[index : index+8]
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			log.Fatalf("error converting buf to lenth %v", err)
		}
		// 移动指针
		index += 8
		dataArea := data[index : index+dataLen]
		var value kv.Value
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			log.Fatalf("error unmarshalling value %v", err)
		}
		if value.Deleted {
			// list.Delete(value.Key)
			preList.Delete(value.Key)
		} else {
			// list.Insert(value.Key, value.Value)
			preList.Insert(value.Key, value.Value)
		}
		index += dataLen
	}
	return preList
}

// 写WAL
func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if value.Deleted {
		log.Printf("wal.write: delete %v", value.Key)
	} else {
		log.Printf("wal.write: insert %v", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, uint32(len(data)))
	if err != nil {
		log.Printf("wal.write: write error: %v", err)
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		log.Printf("wal.write: write error: %v", err)
	}
}

func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()
	log.Println("Resetting wal.log...")
	err := w.f.Close()
	if err != nil {
		log.Fatalf("error closing wal.log: %v", err)
	}
	w.f = nil
	err = os.Remove(w.path)
	if err != nil {
		log.Fatalf("error removing wal.log: %v", err)
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error re-creating wal.log file: %v", err)
	}
	w.f = f
}

func (w *Wal) DeleteFile() {
	w.lock.Lock()
	defer w.lock.Unlock()
	log.Println("Deleting wal.log...")
	err := w.f.Close()
	if err != nil {
		log.Fatalf("error closing wal.log: %v", err)
	}
	err = os.Remove(w.path)
	if err != nil {
		log.Fatalf("error removing wal.log: %v", err)
	}
}
