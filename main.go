package main

import (
	"flag"
)

var (
	source   = flag.String("s", "", "Source Files Directory to be copied")
	host     = flag.String("h", "", "Hostname for destination PSQL")
	db       = flag.String("d", "", "Database for destination PSQL")
	password = flag.String("p", "", "Password for destination PSQL user postgres")
	exchange = flag.String("e", "", "Exchange for Fill")
	threads  = flag.Int("threads", 20, "Number of Threads for Parallel Processing")
)

type Config struct {
	Host string
	Port string
	User string
	Pwd  string
	Name string
}

func main() {
	var config Config
	flag.Parse()
	config.Host = *host
	config.Name = *db
	config.Port = "5432"
	config.Pwd = *password
	config.User = "postgres"
	Go(*source, config, *exchange, *threads)

}
