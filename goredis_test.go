package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/xiaoxuxiansheng/goredis/app"
	"github.com/xiaoxuxiansheng/goredis/lib"
	"github.com/xiaoxuxiansheng/goredis/lib/pool"
)

// goredis 质检员
type QualityInspector struct {
	times  int
	app    *app.Application
	t      *testing.T
	rander *rand.Rand
}

func NewQualityInspector(t *testing.T, times int) *QualityInspector {
	return &QualityInspector{
		t:      t,
		times:  times,
		rander: rand.New(rand.NewSource(lib.TimeNow().UnixNano())),
	}
}

func (q *QualityInspector) prepareApp(clean bool) error {
	// 1 移除 aof 文件，避免读取脏数据
	if clean {
		_ = os.Remove("./appendonly.aof")
	}
	// 1 创建应用
	server, err := app.ConstructServer()
	if err != nil {
		return err
	}
	q.app = app.NewApplication(server, app.SetUpConfig())
	// 2 异步启动应用
	pool.Submit(func() {
		if err := q.app.Run(); err != nil {
			q.t.Error(err)
		}
	})
	return nil
}

func (q *QualityInspector) connApp() (*net.TCPConn, error) {
	<-time.After(100 * time.Millisecond)
	// 建立 tcp 连接
	return net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 6379,
	})
}

func (q *QualityInspector) execSet(w io.Writer) {
	writer := bufio.NewWriter(w)
	for i := 0; i < 2*q.times; i++ {
		k := cast.ToString(i % q.times)
		v := cast.ToString(i % q.times)
		_, _ = writer.WriteString("*3\r\n")
		_, _ = writer.WriteString("$3\r\n")
		_, _ = writer.WriteString("set\r\n")
		_, _ = writer.WriteString(fmt.Sprintf("$%d\r\n", len(k)))
		_, _ = writer.WriteString(fmt.Sprintf("%s\r\n", k))
		_, _ = writer.WriteString(fmt.Sprintf("$%d\r\n", len(v)))
		_, _ = writer.WriteString(fmt.Sprintf("%s\r\n", v))
		if err := writer.Flush(); err != nil {
			q.t.Error(err)
		}
		q.t.Logf("set k: %s, v: %s", k, v)
	}
}

func (q *QualityInspector) readSetResp(r io.Reader) {
	reader := bufio.NewReader(r)
	for i := 0; i < q.times; i++ {
		line, _, _ := reader.ReadLine()
		q.t.Logf("set resp: %s\n", line)
	}
}

func (q *QualityInspector) execGet(w io.Writer) {
	writer := bufio.NewWriter(w)
	for i := 0; i < q.times; i++ {
		k := cast.ToString(i)
		_, _ = writer.WriteString("*2\r\n")
		_, _ = writer.WriteString("$3\r\n")
		_, _ = writer.WriteString("get\r\n")
		_, _ = writer.WriteString(fmt.Sprintf("$%d\r\n", len(k)))
		_, _ = writer.WriteString(fmt.Sprintf("%s\r\n", k))
		if err := writer.Flush(); err != nil {
			q.t.Error(err)
		}
		q.t.Logf("get k: %s", k)
	}
}

func (q *QualityInspector) readGetResp(r io.Reader) {
	reader := bufio.NewReader(r)
	for i := 0; i < q.times; i++ {
		_, _, _ = reader.ReadLine() // $n
		line, _, _ := reader.ReadLine()
		q.t.Logf("get resp: %s\n", line)
		assert.Equal(q.t, cast.ToString(i), string(line))
	}
}

func Test_Goredis_Set_Get(t *testing.T) {
	q := NewQualityInspector(t, 100)

	// 1 启动 go redis. 保证全局唯一
	if err := q.prepareApp(true); err != nil {
		t.Error(err)
		return
	}
	defer q.app.Stop()

	// 2 连接到 go redis
	conn, err := q.connApp()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	var wg sync.WaitGroup
	// 3 读取set结果
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.readSetResp(conn)
	})

	// 4 发送set指令
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.execSet(conn)
	})

	wg.Wait()

	// 5 读取get结果
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.readGetResp(conn)
	})

	// 6 发送get指令
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.execGet(conn)
	})
	wg.Wait()

	<-time.After(time.Second)
}

func Test_Goredis_Set(t *testing.T) {
	test_goredis_set(t) // 1 启动 goredis  2 set 数据 3 停止 goredis
}

func test_goredis_set(t *testing.T) {
	q := NewQualityInspector(t, 100)

	// 1 启动 go redis. 保证全局唯一
	if err := q.prepareApp(true); err != nil {
		t.Error(err)
		return
	}
	defer q.app.Stop()

	// 2 连接到 go redis
	conn, err := q.connApp()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	var wg sync.WaitGroup
	// 3 读取set结果
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.readSetResp(conn)
	})

	// 4 发送set指令
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.execSet(conn)
	})

	wg.Wait()
	<-time.After(2 * time.Second)
}

func Test_Aof_Get(t *testing.T) {
	test_goredis_aof_get(t) // 1 启动 goredis（通过 aof 恢复数据）2 get 数据 3 停止 goredis
}

func test_goredis_aof_get(t *testing.T) {
	q := NewQualityInspector(t, 100)

	<-time.After(time.Second)
	// 1 启动 go redis. 不删除 aof 文件
	if err := q.prepareApp(false); err != nil {
		t.Error(err)
		return
	}
	defer q.app.Stop()

	// 2 连接到 go redis
	<-time.After(time.Second)
	conn, err := q.connApp()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	var wg sync.WaitGroup
	// 5 读取get结果
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.readGetResp(conn)
	})

	// 6 发送get指令
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		q.execGet(conn)
	})
	wg.Wait()
}
