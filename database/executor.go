package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/xiaoxuxiansheng/goredis/handler"
)

type DBExecutor struct {
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan *Command

	cmdHandlers map[CmdType]CmdHandler
	dataStore   DataStore
}

func NewDBExecutor(dataStore DataStore) *DBExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	e := DBExecutor{
		dataStore: dataStore,
		ch:        make(chan *Command),
		ctx:       ctx,
		cancel:    cancel,
	}
	e.cmdHandlers = map[CmdType]CmdHandler{
		CmdTypeExpire: e.dataStore.Expire,

		// string
		CmdTypeGet:  e.dataStore.Get,
		CmdTypeSet:  e.dataStore.Set,
		CmdTypeMGet: e.dataStore.MGet,
		CmdTypeMSet: e.dataStore.MSet,

		// list
		CmdTypeLPush:  e.dataStore.LPush,
		CmdTypeLPop:   e.dataStore.LPop,
		CmdTypeRPush:  e.dataStore.RPush,
		CmdTypeRPop:   e.dataStore.RPop,
		CmdTypeLRange: e.dataStore.LRange,

		// set
		CmdTypeSAdd:      e.dataStore.SAdd,
		CmdTypeSIsMember: e.dataStore.SIsMember,
		CmdTypeSRem:      e.dataStore.SRem,

		// hash
		CmdTypeHSet: e.dataStore.HSet,
		CmdTypeHGet: e.dataStore.HGet,
		CmdTypeHDel: e.dataStore.HDel,

		// sorted set
		CmdTypeZAdd:          e.dataStore.ZAdd,
		CmdTypeZRangeByScore: e.dataStore.ZRangeByScore,
		CmdTypeZRem:          e.dataStore.ZRem,
	}

	go e.run()
	return &e
}

func (e *DBExecutor) Entrance() chan<- *Command {
	return e.ch
}

func (e *DBExecutor) ValidCommand(cmd CmdType) bool {
	_, valid := e.cmdHandlers[cmd] // map 只读，不考虑并发问题
	return valid
}

func (e *DBExecutor) Close() {
	e.cancel()
}

func (e *DBExecutor) run() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case cmd := <-e.ch:
			cmdFunc, ok := e.cmdHandlers[CmdType(strings.ToLower(cmd.cmd.String()))]
			if !ok {
				cmd.receiver <- handler.NewErrReply(fmt.Sprintf("unknown command '%s'", cmd.cmd))
				continue
			}
			cmd.receiver <- cmdFunc(cmd.args)
		}
	}
}
