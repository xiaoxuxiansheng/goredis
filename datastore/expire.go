package datastore

import "time"

func (k *KVStore) expire(key string, expiredAt time.Time) {
	if _, ok := k.data[key]; !ok {
		return
	}
	k.expiredAt[key] = expiredAt
}
