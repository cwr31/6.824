package main

import (
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	log.Printf(os.TempDir())
	mu := sync.Mutex{}
	mu.Lock()
	go func() {
		log.Printf("%+v", mu)
		mu.Lock()
	}()
	go func() {
		log.Printf("%+v", mu)
		mu.Lock()
	}()
	go func() {
		log.Printf("%+v", mu)
		mu.Lock()
	}()
	log.Printf("%+v", mu)
	time.Sleep(time.Millisecond * 10000)
	asd()
	time.Sleep(time.Millisecond * 10000)
}

func asd() {
	log.Printf("asd")
}

func tickSchedule() {

	for {
		log.Printf("just tick it")
		go schedule()
		//<- m.sch
		time.Sleep(time.Millisecond * 1000)
	}
}

func schedule() {
	log.Printf("schedule")
}
