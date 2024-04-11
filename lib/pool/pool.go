package pool

import (
	"runtime/debug"
	"strings"

	"github.com/xiaoxuxiansheng/goredis/log"

	"github.com/panjf2000/ants"
)

var pool *ants.Pool

func init() {
	_pool, err := ants.NewPool(50000, ants.WithPanicHandler(func(i interface{}) {
		stackInfo := strings.Replace(string(debug.Stack()), "\n", "", -1)
		log.GetDefaultLogger().Errorf("recover info: %v, stack info: %s", i, stackInfo)
	}))
	if err != nil {
		panic(err)
	}
	pool = _pool
}

func Submit(task func()) {
	pool.Submit(task)
}
