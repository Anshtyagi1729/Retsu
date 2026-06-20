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
	"strconv"
	"syscall"
	"time"
)

type config struct {
	redisAddr       string
	httpAddr        string
	poolSize        int
	shutdownTimeout time.Duration
	inflightTimeout time.Duration
}

func loadConfig() config {
	return config{
		redisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		httpAddr:        getEnv("HTTP_ADDR", ":8080"),
		poolSize:        getEnvInt("WORKER_POOL_SIZE", 20),
		shutdownTimeout: time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_SECONDS", 10)) * time.Second,
		inflightTimeout: time.Duration(getEnvInt("INFLIGHT_TIMEOUT_SECONDS", 300)) * time.Second,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%q, using default %d", key, v, fallback)
		return fallback
	}
	return n
}

func main() {
	fmt.Println("retsu is alive now ")
	cfg := loadConfig()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	q := queue.New(cfg.redisAddr)
	w := worker.New(q)
	s := server.New(q)
	go func() {
		log.Printf("API listnign on %s", cfg.httpAddr)
		if err := s.Start(cfg.httpAddr); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	go w.RunScheduler(ctx)
	go w.RunWatchdawg(ctx, cfg.inflightTimeout)
	go func() {
		<-ctx.Done()
		log.Println("shutdown signal received, draining in-flight HTTP requests")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
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
	log.Printf("workers are starting (pool size %d)", cfg.poolSize)
	w.StartPool(ctx, cfg.poolSize)
	log.Println("shutdown complete")
}
