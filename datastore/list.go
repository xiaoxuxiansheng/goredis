package datastore

import (
	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/handler"
)

func (k *KVStore) getAsList(key string) (List, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	list, ok := v.(List)
	if !ok {
		return nil, handler.NewWrongTypeErrReply()
	}

	return list, nil
}

func (k *KVStore) putAsList(key string, list List) {
	k.data[key] = list
}

type List interface {
	LPush(value []byte)
	LPop(cnt int64) [][]byte
	RPush(value []byte)
	RPop(cnt int64) [][]byte
	Len() int64
	Range(start, stop int64) [][]byte
	database.CmdAdapter
}

type listEntity struct {
	key  string
	data [][]byte
}

func newListEntity(key string, elements ...[]byte) List {
	return &listEntity{
		key:  key,
		data: elements,
	}
}

func (l *listEntity) LPush(value []byte) {
	l.data = append([][]byte{value}, l.data...)
}

func (l *listEntity) LPop(cnt int64) [][]byte {
	if int64(len(l.data)) < cnt {
		return nil
	}

	poped := l.data[:cnt]
	l.data = l.data[cnt:]
	return poped
}

func (l *listEntity) RPush(value []byte) {
	l.data = append(l.data, value)
}

func (l *listEntity) RPop(cnt int64) [][]byte {
	if int64(len(l.data)) < cnt {
		return nil
	}

	poped := l.data[int64(len(l.data))-cnt:]
	l.data = l.data[:int64(len(l.data))-cnt]
	return poped
}

func (l *listEntity) Len() int64 {
	return int64(len(l.data))
}

func (l *listEntity) Range(start, stop int64) [][]byte {
	if stop == -1 {
		stop = int64(len(l.data) - 1)
	}

	if start < 0 || start >= int64(len(l.data)) {
		return nil
	}

	if stop < 0 || stop >= int64(len(l.data)) || stop < start {
		return nil
	}

	return l.data[start : stop+1]
}

func (l *listEntity) ToCmd() [][]byte {
	args := make([][]byte, 0, 2+l.Len())
	args = append(args, []byte(database.CmdTypeRPush), []byte(l.key))
	args = append(args, l.data...)
	return args
}
