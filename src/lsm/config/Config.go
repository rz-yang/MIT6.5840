package config

import "sync"

// 配置文件
type Config struct {
	// 数据目录
	DataDir string
	// 0 层的最大SSTable容量，单位为MB，超过该值会压入下一层，进行压缩操作
	Level0Size int
	// 层间的容量倍数
	GrowTimes int
	// 最大level层级
	MaxLevel int
	// 每层中 SsTable 表数量的阈值，该层 SsTable 将会被压缩到下一层
	PartSize int
	// 周期性check
	CheckInterval int
	// 设置的最大转为immutable的threshold，这里的定义为元素数量
	Threshold int
}

var once *sync.Once = &sync.Once{}

var config Config

var defaultConfig = Config{
	DataDir:       "./data",
	Level0Size:    10,
	GrowTimes:     3,
	MaxLevel:      10,
	PartSize:      1024 * 1024,
	CheckInterval: 5,
	Threshold:     1024,
}

func init() {
	config = Config{}
}

func SetConfig(con Config) {
	config.DataDir = con.DataDir
	config.Level0Size = con.Level0Size
	config.GrowTimes = con.GrowTimes
	config.MaxLevel = con.MaxLevel
	config.PartSize = con.PartSize
	config.CheckInterval = con.CheckInterval
	config.Threshold = con.Threshold
}

func GetConfig() Config {
	return config
}

func GetLevelMaxSize(level int) int {
	return config.Level0Size * pow(config.GrowTimes, level)
}

func pow(a, b int) int {
	ans := 0
	for b > 0 {
		if (b & 1) == 1 {
			ans = ans * a
		}
		b >>= 1
		a = a * a
	}
	return ans
}
