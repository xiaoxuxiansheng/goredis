package database

type Persister struct{}

func NewPersisiter() *Persister {
	return &Persister{}
}

// 重写 aof 文件
func (p *Persister) RewriteAOF() error {
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
