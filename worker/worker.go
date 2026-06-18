package worker

import (
	"context"
	"retsu/queue"
)

type Worker struct{
	queue *queue.Queue
}
func New(q *queue.Queue) *Worker{
	return &Worker{queue :q }
}
func (w *Worker) Start(ctx context.Context){
	for{
		select{
			case<-ctx.Done():
				return
			default:
				var j job
		}
	}
}