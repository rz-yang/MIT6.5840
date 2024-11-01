package ssTable

import (
	"log"
	"os"
	"path"
	"raft_LSMTree-based_KVStore/lsm/config"
	"time"
)

// 初始化LevelTree
func (tree *LevelTree) Init(dir string) {
	log.Println("The SSTable are being loaded")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Time elapsed: %s\n", elapsed)
	}()

	con := config.GetConfig()
	levelCnt := con.MaxLevel
	tree.InitWithParam(levelCnt)
	fileInfo, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, info := range fileInfo {
		if path.Ext(info.Name()) == ".db" {
			tree.loadDbFile(path.Join(dir, info.Name()))
		}
	}
}
