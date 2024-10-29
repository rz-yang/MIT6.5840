package kv

import "encoding/json"

type Value struct {
	Key     string
	Value   []byte
	Deleted bool
}

// 拷贝
func (v *Value) Copy() *Value {
	return &Value{
		Key:     v.Key,
		Value:   v.Value,
		Deleted: v.Deleted,
	}
}

// 返回json编码
func Encode(value Value) ([]byte, error) {
	return json.Marshal(value)
}

// 解析返回Data
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}
