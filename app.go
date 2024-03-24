package goredis

type Server interface {
	ListenAndServe(address string) error
}
type Application struct {
	server Server
	conf   *Config
}

func NewApplication(server Server, conf *Config) *Application {
	return &Application{
		server: server,
		conf:   conf,
	}
}

func (a *Application) Run() error {
	return a.server.ListenAndServe(a.conf.Address)
}
