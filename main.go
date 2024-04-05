package main

import "github.com/xiaoxuxiansheng/goredis/app"

func main() {
	server, err := app.ConstructServer()
	if err != nil {
		panic(err)
	}

	app := app.NewApplication(server, &app.Config{
		Address: ":6379",
	})
	if err := app.Run(); err != nil {
		panic(err)
	}
}
