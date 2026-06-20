package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"retsu/queue"
	"retsu/server"
	"retsu/worker"
	"syscall"
	"time"
)

const shutdownTimeout = 10 * time.Second

func main() {
	fmt.Println("retsu is alive now ")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	q := queue.New("localhost:6379")
	w := worker.New(q)
	s := server.New(q)
	go func() {
		log.Printf("API listnign on :8080")
		if err := s.Start(":8080"); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	go w.RunScheduler(ctx)
	go w.RunWatchdawg(ctx)
	go func() {
		<-ctx.Done()
		log.Println("shutdown signal received, draining in-flight HTTP requests")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown error: %v", err)
		}
		// workers are blocked in a Block:0 (forever) read - cancellation alone
		// can't interrupt that, only closing the connection can.
		if err := q.Close(); err != nil {
			log.Printf("redis close error: %v", err)
		}
	}()
	log.Println("workers are starting")
	w.StartPool(ctx, 5)
	log.Println("shutdown complete")
}
