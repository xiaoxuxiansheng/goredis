package persist

import (
	"context"
	"os"
	"sync"

	"github.com/xiaoxuxiansheng/goredis/datastore"
	"github.com/xiaoxuxiansheng/goredis/handler"
)

type aofPersister struct {
	ctx    context.Context
	cancel context.CancelFunc

	entrance    chan [][]byte
	aofFile     *os.File
	aofFileName string

	mu sync.Mutex

	once sync.Once
}

func NewAofPersister() (datastore.Persister, error) {
	aofFileName := "./redis.aof"
	aofFile, err := os.OpenFile(aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := aofPersister{
		ctx:         ctx,
		cancel:      cancel,
		entrance:    make(chan [][]byte),
		aofFile:     aofFile,
		aofFileName: aofFileName,
	}
	go a.run()
	return &a, nil
}

func (a *aofPersister) Reload() *datastore.KVStore {
	// 读取指令，还原出 kv store
	return nil
}

func (a *aofPersister) PersistCmd(cmd [][]byte) {
	a.entrance <- cmd
}

func (a *aofPersister) Close() {
	a.once.Do(func() {
		a.cancel()
		_ = a.aofFile.Close()
	})
}

func (a *aofPersister) run() {
	for {
		select {
		case <-a.ctx.Done():
			// log
			return
		case cmd := <-a.entrance:
			a.writeAof(cmd)
		}
	}
}

func (a *aofPersister) writeAof(cmd [][]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()

	persistCmd := handler.NewMultiBulkReply(cmd)
	if _, err := a.aofFile.Write(persistCmd.ToBytes()); err != nil {
		// log
	}

	_ = a.aofFile.Sync()
}
