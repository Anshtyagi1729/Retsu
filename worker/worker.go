package worker

import (
	"context"
	"encoding/json"
	"log"
	"retsu/job"
	"retsu/processor"
	"retsu/queue"
	"sync"
	"time"
)

type Worker struct {
	queue *queue.Queue
}

func New(q *queue.Queue) *Worker {
	return &Worker{queue: q}
}
func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			data, err := w.queue.Pop(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("worker says: pop failed:%v", err)
				time.Sleep(time.Second)
				continue
			}
			err = w.queue.MoveToInflight(ctx, data)
			if err != nil {
				log.Printf("worker says:mtif failed:%v", err)
				time.Sleep(time.Second)
				continue
			}
			var j job.Job
			if err := json.Unmarshal(data, &j); err != nil {
				log.Printf("worker says:decode failed:%v", err)
				continue
			}
			j.Attempts++
			err = processor.Process(j)
			if err != nil {
				w.queue.Fail(ctx, j)
				continue
			}
			w.queue.Ack(ctx, data)
		}
	}
}
func (w *Worker) StartPool(ctx context.Context, n int) {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Start(ctx)
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
