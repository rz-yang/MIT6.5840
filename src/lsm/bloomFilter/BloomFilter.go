package bloomFilter

import "log"

type BloomFilter struct {
	bitSet   []bool
	hashFunc []func(str string) int
	size     int
}

var defaultBloomSize = 1024 * 1024

var defaultHashFunc = []func(str string) int{
	func(str string) int {
		return len(str)
	},
	func(str string) int {
		hash := 0
		for i := 0; i < len(str); i++ {
			hash = (hash*131 + int(str[i]-'0')) % defaultBloomSize
		}
		return hash
	},
	func(str string) int {
		hash := 0
		for i := 0; i < len(str); i++ {
			hash = (hash*1031 + int(str[i]-'0')*2) % defaultBloomSize
		}
		return hash
	},
}

var defaultBloomFilter = NewBloomFilter(defaultBloomSize, defaultHashFunc)

func GetDefaultBloomFilter() *BloomFilter {
	return defaultBloomFilter
}

func NewBloomFilter(size int, funcs []func(str string) int) *BloomFilter {
	return &BloomFilter{
		bitSet:   make([]bool, size),
		hashFunc: funcs,
		size:     size,
	}
}

func (bf *BloomFilter) Add(str string) {
	log.Println("BloomFilter添加元素key ", str)
	for _, hash := range bf.hashFunc {
		bf.bitSet[hash(str)%bf.size] = true
	}
}

func (bf *BloomFilter) Exists(str string) bool {
	for _, hash := range bf.hashFunc {
		if !bf.bitSet[hash(str)%bf.size] {
			log.Println("BloomFilter过滤key ", str, " key不存在")
			return false
		}
	}
	log.Println("BloomFilter元素key ", str, " 可能存在")
	return true
}
