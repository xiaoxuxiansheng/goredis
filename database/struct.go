package database

import (
	"context"
	"strings"
	"time"

	"github.com/xiaoxuxiansheng/goredis/handler"
)

type Executor interface {
	Entrance() chan<- *Command
	ValidCommand(cmd CmdType) bool
	Close()
}

type CmdType string

func (c CmdType) String() string {
	return strings.ToLower(string(c))
}

const (
	CmdTypeExpire   CmdType = "expire"
	CmdTypeExpireAt CmdType = "expireat"

	// string
	CmdTypeGet  CmdType = "get"
	CmdTypeSet  CmdType = "set"
	CmdTypeMGet CmdType = "mget"
	CmdTypeMSet CmdType = "mset"

	// list
	CmdTypeLPush  CmdType = "lpush"
	CmdTypeLPop   CmdType = "lpop"
	CmdTypeRPush  CmdType = "rpush"
	CmdTypeRPop   CmdType = "rpop"
	CmdTypeLRange CmdType = "lrange"

	// hash
	CmdTypeHSet CmdType = "hset"
	CmdTypeHGet CmdType = "hget"
	CmdTypeHDel CmdType = "hdel"

	// set
	CmdTypeSAdd      CmdType = "sadd"
	CmdTypeSIsMember CmdType = "sismember"
	CmdTypeSRem      CmdType = "srem"

	// sorted set
	CmdTypeZAdd          CmdType = "zadd"
	CmdTypeZRangeByScore CmdType = "zrangebyscore"
	CmdTypeZRem          CmdType = "zrem"
)

type CmdAdapter interface {
	ToCmd() [][]byte
}

type DataStore interface {
	ForEach(task func(key string, adapter CmdAdapter, expireAt *time.Time))

	ExpirePreprocess(key string)
	GC()

	Expire(*Command) handler.Reply
	ExpireAt(*Command) handler.Reply

	// string
	Get(*Command) handler.Reply
	MGet(*Command) handler.Reply
	Set(*Command) handler.Reply
	MSet(*Command) handler.Reply

	// list
	LPush(*Command) handler.Reply
	LPop(*Command) handler.Reply
	RPush(*Command) handler.Reply
	RPop(*Command) handler.Reply
	LRange(*Command) handler.Reply

	// set
	SAdd(*Command) handler.Reply
	SIsMember(*Command) handler.Reply
	SRem(*Command) handler.Reply

	// hash
	HSet(*Command) handler.Reply
	HGet(*Command) handler.Reply
	HDel(*Command) handler.Reply

	// sorted set
	ZAdd(*Command) handler.Reply
	ZRangeByScore(*Command) handler.Reply
	ZRem(*Command) handler.Reply
}

type CmdHandler func(*Command) handler.Reply

type Command struct {
	ctx      context.Context
	cmd      CmdType
	args     [][]byte
	receiver CmdReceiver
}

func NewCommand(cmd CmdType, args [][]byte) *Command {
	return &Command{
		cmd:  cmd,
		args: args,
	}
}

func (c *Command) Ctx() context.Context {
	return c.ctx
}

func (c *Command) Receiver() CmdReceiver {
	return c.receiver
}

func (c *Command) Args() [][]byte {
	return c.args
}

func (c *Command) Cmd() [][]byte {
	return append([][]byte{[]byte(c.cmd.String())}, c.args...)
}

type CmdReceiver chan handler.Reply
