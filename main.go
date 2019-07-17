package main

import (
	"flag"
	"github.com/sak0/esCleaner/pkg/es"
	"strings"
	"os"
	"os/signal"
	"syscall"
	)

var (
	indexName		string
	timeField 		string
	dateEnd			int
	dateStart 		int
	esAddrString 	string
)

func init() {
	flag.StringVar(&indexName, "index", "testIndex", "index name to clean.")
	flag.StringVar(&timeField, "field", "timestamp", "filed for time.")
	flag.IntVar(&dateEnd, "end", 20180529, "end date for clean.")
	flag.StringVar(&esAddrString, "es", "http://192.168.1.1:9200,http://192.168.1.1:9200,http://192.168.1.1:9200",
		"es cluster addr")
	flag.IntVar(&dateStart, "start", 20180501, "start date to clean")

	flag.Parse()
}

func main() {
	worker, err := es.New(strings.Split(esAddrString, ","), indexName, timeField, dateStart, dateEnd)
	if err != nil {
		panic(err)
	}

	stop := make(chan interface{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGKILL|syscall.SIGTERM)
	go func() {
		<-sigCh
		close(stop)
	}()

	worker.Run(stop)
}