package datastore

import "github.com/xiaoxuxiansheng/goredis/handler"

func (k *KVStore) getAsHashMap(key string) (HashMap, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	hmap, ok := v.(HashMap)
	if !ok {
		return nil, handler.NewWrongTypeErrReply()
	}

	return hmap, nil
}

func (k *KVStore) putAsHashMap(key string, hmap HashMap) {
	k.data[key] = hmap
}

type HashMap interface {
	Put(key string, value []byte)
	Get(key string) []byte
	Del(key string) int64
}

type hashMapEntity struct {
	data map[string][]byte
}

func newHashMapEntity() HashMap {
	return &hashMapEntity{
		data: make(map[string][]byte),
	}
}

func (h *hashMapEntity) Put(key string, value []byte) {
	h.data[key] = value
}

func (h *hashMapEntity) Get(key string) []byte {
	return h.data[key]
}

func (h *hashMapEntity) Del(key string) int64 {
	if _, ok := h.data[key]; !ok {
		return 0
	}
	delete(h.data, key)
	return 1
}
