package goredis

func main() {
	server, err := constructServer()
	if err != nil {
		panic(err)
	}

	app := NewApplication(server, &Config{})
	if err := app.Run(); err != nil {
		panic(err)
	}
}
