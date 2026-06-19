package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"retsu/queue"
	"retsu/server"
	"retsu/worker"
)

func main() {
	fmt.Println("retsu is alive now ")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	q := queue.New("localhost:6379")
	w := worker.New(q)
	s := server.New(q)
	go func() {
		log.Printf("API listnign on :8080")
		if err := s.Start(":8080"); err != nil {
			log.Fatal(err)
		}
	}()
	go w.RunScheduler(ctx)
	go w.RunWatchdawg(ctx)
	log.Println("workers are starting")
	w.StartPool(ctx, 5)
	log.Println("shutdown")

}
