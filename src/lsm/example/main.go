package main

import (
	"fmt"
	"raft_LSMTree-based_KVStore/lsm"
	"raft_LSMTree-based_KVStore/lsm/config"
	"time"
)

type TestValue struct {
	Key string
	A   int64
	B   int64
	C   int64
	D   string
}

func main() {
	testResetWalBug()
	for {
		time.Sleep(10 * time.Second)
	}
}

func testResetWalBug() {
	// lsm.Start(config.GetDefaultConfig())
	lsm.Start(config.Config{
		DataDir:       "./data",
		Level0Size:    1,
		PartSize:      4,
		Threshold:     1000,
		CheckInterval: 2,
		GrowTimes:     10,
		MaxLevel:      10,
	})
	//insertValuesByCount(20000, 10)
	//time.Sleep(2 * time.Second)
	//insertValuesByCount(6, 0)
	keys := []string{"3", "15", "16", "20", "30", "40", "5000"}
	queryByKeys(keys)
	time.Sleep(10 * time.Second)
}

func queryByKeys(keys []string) {
	for _, key := range keys {
		start := time.Now()
		v, exists := lsm.Get[TestValue](key)
		if exists {
			fmt.Println("已找到，value为", v)
		} else {
			fmt.Printf("未找到key %v对应的键值对\n", key)
		}
		elapse := time.Since(start)
		fmt.Println("查找", key, " 完成，消耗时间：", elapse)
	}
}

func query(key string) {
	start := time.Now()
	v, _ := lsm.Get[TestValue]("4")
	elapse := time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)

	start = time.Now()
	v, exists := lsm.Get[TestValue](key)
	if exists {
		fmt.Println(v)
	} else {
		fmt.Printf("未找到key %v对应的键值对\n", key)
	}
	elapse = time.Since(start)
	fmt.Println("查找 aazzzz 完成，消耗时间：", elapse)

}

func insertValuesByCount(count, startFrom int) {
	start := time.Now()
	// 64 个字节
	testV := TestValue{
		C: 3,
		D: "hello world",
	}
	for i := 0; i < count; i++ {
		testV.A = int64(i + startFrom)
		testV.B = int64(i + startFrom)
		testV.Key = fmt.Sprint(i + startFrom)
		lsm.Set(testV.Key, testV)
	}
	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, ",消耗时间：", elapse)
}

func insert() {
	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}

	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a', 'a', 'a'}
	lsm.Set(string(key), testV)
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					for e := 0; e < 26; e++ {
						for f := 0; f < 26; f++ {
							key[0] = 'a' + byte(a)
							key[1] = 'a' + byte(b)
							key[2] = 'a' + byte(c)
							key[3] = 'a' + byte(d)
							key[4] = 'a' + byte(e)
							key[5] = 'a' + byte(f)
							lsm.Set(string(key), testV)
							count++
						}
					}
				}
			}
		}
	}
	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, ",消耗时间：", elapse)
}
