package database

import "github.com/xiaoxuxiansheng/goredis/handler"

type Executor interface {
	Entrance() chan<- *Command
	ValidCommand(cmd CmdType) bool
	Close()
}

type CmdType string

const (
	// string
	CmdTypeGet  CmdType = "get"
	CmdTypeSet  CmdType = "set"
	CmdTypeMGet CmdType = "mget"
	CmdTypeMSet CmdType = "mset"

	// list
	CmdTypeLPush CmdType = "lpush"
	CmdTypeLPop  CmdType = "lpop"

	// hash
	CmdTypeHSet CmdType = "hset"
	CmdTypeHGet CmdType = "hget"
	CmdTypeHDel CmdType = "hdel"

	// set
	CmdTypeSAdd      CmdType = "sadd"
	CmdTypeSIsMember CmdType = "sismember"
	CmdTypeSRem      CmdType = "srem"

	// sorted set
	CmdTypeZAdd   CmdType = "zadd"
	CmdTypeZRange CmdType = "zrange"
	CmdTypeZRem   CmdType = "zrem"
)

type DataStore interface {
	// string
	Get(args [][]byte) handler.Reply
	MGet(args [][]byte) handler.Reply
	Set(args [][]byte) handler.Reply
	MSet(args [][]byte) handler.Reply

	// list
	LPush(args [][]byte) handler.Reply
	LPop(args [][]byte) handler.Reply

	// set
	SAdd(args [][]byte) handler.Reply
	SIsMember(args [][]byte) handler.Reply
	SRem(args [][]byte) handler.Reply

	// hash
	HSet(args [][]byte) handler.Reply
	HGet(args [][]byte) handler.Reply
	HDel(args [][]byte) handler.Reply

	// sorted set
	ZAdd(args [][]byte) handler.Reply
	ZRange(args [][]byte) handler.Reply
	ZRem(args [][]byte) handler.Reply
}

type CmdHandler func(args [][]byte) handler.Reply

type Command struct {
	cmd      CmdType
	args     [][]byte
	receiver CmdReceiver
}

func (c *Command) Receiver() CmdReceiver {
	return c.receiver
}

type CmdReceiver chan handler.Reply