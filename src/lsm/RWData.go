package lsm

import (
	"encoding/json"
	"log"
	"raft_LSMTree-based_KVStore/lsm/bloomFilter"
	"raft_LSMTree-based_KVStore/lsm/kv"
)

func Get[T any](key string) (T, bool) {
	log.Println("get key ", key)
	filter := bloomFilter.GetDefaultBloomFilter()
	if !filter.Exists(key) {
		var nilV T
		return nilV, false
	}
	kvValue, isSuccess := database.Search(key)
	val, canConvert := getInstance[T](kvValue.Value)
	return val, isSuccess && canConvert
}

func Set[T any](key string, value T) bool {
	log.Println("set key ", key, " value ", value)
	filter := bloomFilter.GetDefaultBloomFilter()
	filter.Add(key)
	data, err := kv.Convert(value)
	if err != nil {
		log.Println(err)
		return false
	}
	isSuccess := database.Set(key, data)
	return isSuccess
}

func Delete(key string) bool {
	log.Println("delete key ", key)
	isSuccess := database.Delete(key)
	return isSuccess
}

func getInstance[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
		return value, false
	}
	return value, true
}
