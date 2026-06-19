package queue

import (
	"context"
	"encoding/json"
	"retsu/job"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	//this is for redisssssnamesake/ these are the keys
	PendingKey  = "retsu:pending"
	RetryKey    = "retsu:retry"
	InflightKey = "retsu:inflight"
	DLQkey      = "retsu:dlq"
)

type Stats struct {
	Pending  int64 `json:"pending"`
	Inflight int64 `json:"inflight"`
	Retry    int64 `json:"retry"`
	DLQ      int64 `json:"dlq"`
}
type Queue struct {
	client *redis.Client
}

func New(addr string) *Queue {
	client := redis.NewClient(&redis.Options{Addr: addr})
	return &Queue{client: client}
}
func (q *Queue) Push(ctx context.Context, j job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.client.LPush(ctx, PendingKey, data).Err()
}
func (q *Queue) Pop(ctx context.Context) ([]byte, error) {
	data, err := q.client.BRPop(ctx, 0, PendingKey).Result()
	if err != nil {
		return nil, err
	}
	// var j job.Job
	// err = json.Unmarshal([]byte(data[1]), &j)

	return []byte(data[1]), nil
}
func (q *Queue) MoveToInflight(ctx context.Context, data []byte) error {
	score := float64(time.Now().Add(5 * time.Minute).Unix())
	// data, err := json.Marshal(j)
	return q.client.ZAdd(ctx, InflightKey, redis.Z{
		Score:  score,
		Member: data,
	}).Err()
}
func (q *Queue) Ack(ctx context.Context, data []byte) error {
	return q.client.ZRem(ctx, InflightKey, data).Err()
}
func (q *Queue) Fail(ctx context.Context, j job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if j.Attempts >= j.MaxAttempts {
		return q.client.LPush(ctx, DLQkey, data).Err()
	}
	delay := time.Duration(10<<j.Attempts) * time.Second
	score := float64(time.Now().Add(delay).Unix())
	return q.client.ZAdd(ctx, RetryKey, redis.Z{
		Score:  score,
		Member: data,
	}).Err()

}
func (q *Queue) Schedule(ctx context.Context) error {
	data, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     RetryKey,
		Start:   "0",
		Stop:    strconv.FormatInt(time.Now().Unix(), 10),
		ByScore: true,
	}).Result()
	if err != nil {
		return err
	}
	for _, j := range data {
		err := q.client.LPush(ctx, PendingKey, j).Err()
		if err != nil {
			return err
		}
		err = q.client.ZRem(ctx, RetryKey, j).Err()
		if err != nil {
			return err
		}
	}
	return nil
}
func (q *Queue) InflightCleanup(ctx context.Context) error {
	data, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     InflightKey,
		Start:   "0",
		Stop:    strconv.FormatInt(time.Now().Unix(), 10),
		ByScore: true,
	}).Result()
	if err != nil {
		return err
	}
	for _, j := range data {
		if err := q.client.LPush(ctx, PendingKey, j).Err(); err != nil {
			return err
		}
		if err := q.client.ZRem(ctx, InflightKey, j).Err(); err != nil {
			return err
		}
	}
	return nil
}
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	pending, err := q.client.LLen(ctx, PendingKey).Result()
	if err != nil {
		return Stats{}, err
	}
	inflight, err := q.client.ZCard(ctx, InflightKey).Result()
	if err != nil {
		return Stats{}, err
	}
	retry, err := q.client.ZCard(ctx, RetryKey).Result()
	if err != nil {
		return Stats{}, err
	}
	dlq, err := q.client.LLen(ctx, DLQkey).Result()
	if err != nil {
		return Stats{}, err
	}
	return Stats{Pending: pending, Inflight: inflight, Retry: retry, DLQ: dlq}, nil
}
