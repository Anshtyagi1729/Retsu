package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"retsu/job"
	"retsu/processor"
	"retsu/queue"
	"sync"
	"time"
)

const webhookTimeout = 10 * time.Second

type Worker struct {
	queue *queue.Queue
}

func New(q *queue.Queue) *Worker {
	return &Worker{queue: q}
}
func (w *Worker) Start(ctx context.Context, consumer string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := w.queue.ReadGroup(ctx, consumer)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("worker %s says: readgroup failed:%v", consumer, err)
				time.Sleep(time.Second)
				continue
			}
			var j job.Job
			if err := json.Unmarshal(msg.Data, &j); err != nil {
				log.Printf("worker %s says:decode failed:%v", consumer, err)
				w.queue.Ack(ctx, msg.ID)
				continue
			}
			j.Attempts++
			j.Status = job.StatusInFlight
			w.queue.SetJobStatus(ctx, j)

			err = processor.Process(j)
			if err != nil {
				j.LastError = err.Error()
				if j.Attempts >= j.MaxAttempts {
					j.Status = job.StatusDead
				} else {
					j.Status = job.StatusFailed
				}
				w.queue.SetJobStatus(ctx, j)
				w.queue.Fail(ctx, msg.ID, j)
				if j.Status == job.StatusDead {
					go sendWebhook(j)
				}
				continue
			}
			j.Status = job.StatusSucceeded
			w.queue.SetJobStatus(ctx, j)
			w.queue.Ack(ctx, msg.ID)
			w.queue.IncrDone(ctx)
			go sendWebhook(j)
		}
	}
}
// sendWebhook fires a single best-effort POST of the job's final state to
// its callback URL. No retry here - if it fails the client can still poll
// GET /jobs/{id} for the real outcome.
func sendWebhook(j job.Job) {
	if j.CallbackUrl == "" {
		return
	}
	body, err := json.Marshal(j)
	if err != nil {
		log.Printf("webhook %s: encode failed: %v", j.ID, err)
		return
	}
	client := http.Client{Timeout: webhookTimeout}
	resp, err := client.Post(j.CallbackUrl, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("webhook %s: post to %s failed: %v", j.ID, j.CallbackUrl, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("webhook %s: %s returned %s", j.ID, j.CallbackUrl, resp.Status)
	}
}

func (w *Worker) StartPool(ctx context.Context, n int) {
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		consumer := fmt.Sprintf("worker-%d", i)
		go func() {
			defer wg.Done()
			w.Start(ctx, consumer)
		}()
	}
	wg.Wait()
}
func (w *Worker) RunScheduler(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := w.queue.Schedule(ctx); err != nil {
				log.Printf("scheduler says:%v", err)
			}
		}
	}
}
func (w *Worker) RunWatchdawg(ctx context.Context, inflightTimeout time.Duration) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := w.queue.InflightCleanup(ctx, inflightTimeout); err != nil {
				log.Printf("WatchDawg says:%v", err)
			}
		}
	}
}
