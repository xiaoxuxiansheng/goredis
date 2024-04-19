package main

import "github.com/xiaoxuxiansheng/goredis/app"

func main() {
	server, err := app.ConstructServer()
	if err != nil {
		panic(err)
	}

	app := app.NewApplication(server, app.SetUpConfig())
	defer app.Stop()

	if err := app.Run(); err != nil {
		panic(err)
	}
}
