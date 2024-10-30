package config

import "sync"

// 配置文件
type Config struct {
	// 数据目录
	DataDir string
}

var once *sync.Once = &sync.Once{}

var config Config

func Init(con Config) {
	once.Do(func() { config = con })
}

func GetConfig() Config {
	return config
}
