package app

import (
	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/datastore"
	"github.com/xiaoxuxiansheng/goredis/handler"
	"github.com/xiaoxuxiansheng/goredis/log"
	"github.com/xiaoxuxiansheng/goredis/persist"
	"github.com/xiaoxuxiansheng/goredis/protocol"
	"github.com/xiaoxuxiansheng/goredis/server"

	"go.uber.org/dig"
)

var container = dig.New()

func init() {
	// conf
	_ = container.Provide(SetUpConfig)
	_ = container.Provide(PersistThinker)

	// logger
	_ = container.Provide(log.GetDefaultLogger)

	// parser
	_ = container.Provide(protocol.NewParser)

	// persister
	_ = container.Provide(persist.NewAofPersister)

	// datastore
	_ = container.Provide(datastore.NewKVStore)

	// database
	_ = container.Provide(database.NewDBExecutor)
	_ = container.Provide(database.NewDBTrigger)

	// handler
	_ = container.Provide(handler.NewHandler)

	// server
	_ = container.Provide(server.NewServer)
}

func ConstructServer() (*server.Server, error) {
	var s *server.Server
	if err := container.Invoke(func(_s *server.Server) {
		s = _s
	}); err != nil {
		return nil, err
	}
	return s, nil
}
