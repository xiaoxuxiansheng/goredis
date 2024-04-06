package persist

import (
	"context"
	"io"
	"os"
	"sync"

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

func NewAofPersister() (handler.Persister, error) {
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

func (a *aofPersister) Reloader() (io.ReadCloser, error) {
	file, err := os.Open(a.aofFileName)
	if err != nil {
		return nil, err
	}
	return file, nil
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

// 重写 aof 文件
func (a *aofPersister) RewriteAOF() error {
	tmpFile, fileSize, err := a.startRewrite()
	if err != nil {
		return err
	}

	if err = a.doRewrite(tmpFile, fileSize); err != nil {
		return err
	}

	a.endRewrite(tmpFile, fileSize)
	// 1 加锁，短暂暂停 aof 文件写入

	// 2 记录此时 aof 文件的大小， fileSize

	// 3 创建一个临时的 tmp aof 文件，用于进行重写

	// 4 解锁， aof 文件恢复写入

	// 5 读取 fileSize 前的内容，加载到内存副本

	// 6 将内存副本中的内容持久化到 tmp aof 文件中

	// 7 加锁，短暂暂停 aof 文件写入

	// 8 将 aof fileSize 后面的内容拷贝到 tmp aof

	// 9 使用 tmp aof 代替 aof

	// 10 解锁
	return nil
}

func (a *aofPersister) startRewrite() (*os.File, int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.aofFile.Sync(); err != nil {
		return nil, 0, err
	}

	fileInfo, _ := os.Stat(a.aofFileName)
	fileSize := fileInfo.Size()

	// 创建一个临时的 aof 文件
	tmpFile, err := os.CreateTemp("./", "*.aof")
	if err != nil {
		return nil, 0, err
	}

	return tmpFile, fileSize, nil
}

func (a *aofPersister) doRewrite(tmpFile *os.File, fileSize int64) error {
	return nil
}

func (a *aofPersister) endRewrite(tmpFile *os.File, fileSize int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// 以新易旧
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
