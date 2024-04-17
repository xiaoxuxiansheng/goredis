package datastore

import (
	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/handler"
)

func (k *KVStore) getAsString(key string) (String, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	str, ok := v.(String)
	if !ok {
		return nil, handler.NewWrongTypeErrReply()
	}

	return str, nil
}

func (k *KVStore) put(key, value string, insertStrategy bool) int64 {
	if _, ok := k.data[key]; ok && insertStrategy {
		return 0
	}

	k.data[key] = NewString(key, value)
	return 1
}

type String interface {
	Bytes() []byte
	database.CmdAdapter
}

type stringEntity struct {
	key, str string
}

func NewString(key, str string) String {
	return &stringEntity{key: key, str: str}
}

func (s *stringEntity) Bytes() []byte {
	return []byte(s.str)
}

func (s *stringEntity) ToCmd() [][]byte {
	return [][]byte{[]byte(database.CmdTypeSet), []byte(s.key), []byte(s.str)}
}
