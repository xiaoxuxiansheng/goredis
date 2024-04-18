package datastore

import (
	"time"

	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/lib"
)

func (k *KVStore) ForEach(f func(key string, adapter database.CmdAdapter, expireAt *time.Time)) {
	for key, data := range k.data {
		expiredAt, ok := k.expiredAt[key]
		if ok && expiredAt.Before(lib.TimeNow()) {
			continue
		}
		_adapter, _ := data.(database.CmdAdapter)
		if ok {
			f(key, _adapter, &expiredAt)
		} else {
			f(key, _adapter, nil)
		}
	}
}
