package app

import "github.com/xiaoxuxiansheng/goredis/server"

type Application struct {
	server *server.Server
	conf   *Config
}

func NewApplication(server *server.Server, conf *Config) *Application {
	return &Application{
		server: server,
		conf:   conf,
	}
}

func (a *Application) Run() error {
	return a.server.Serve(a.conf.Address())
}

func (a *Application) Stop() {
	a.server.Stop()
}
