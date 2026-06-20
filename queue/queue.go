package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"retsu/job"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	//this is for redisssssnamesake/ these are the keys
	StreamKey  = "retsu:stream"
	GroupName  = "retsu:workers"
	RetryKey   = "retsu:retry"
	DLQkey     = "retsu:dlq"
	dataField  = "data"
	idemKeyFmt = "retsu:idem:%s"
	idemTTL    = 24 * time.Hour
	jobKeyFmt  = "retsu:job:%s"
	jobTTL     = 7 * 24 * time.Hour
)

// ErrJobNotFound is returned by GetJob when no status has been recorded
// for the given ID, or it's aged out past jobTTL.
var ErrJobNotFound = errors.New("job not found")

type Stats struct {
	Pending  int64 `json:"pending"`
	Inflight int64 `json:"inflight"`
	Retry    int64 `json:"retry"`
	DLQ      int64 `json:"dlq"`
}

// Msg is a job pulled off the stream, still tagged with the entry ID the
// consumer group needs to Ack/Claim it.
type Msg struct {
	ID   string
	Data []byte
}

type Queue struct {
	client *redis.Client
}

func New(addr string) *Queue {
	client := redis.NewClient(&redis.Options{Addr: addr})
	q := &Queue{client: client}
	ctx := context.Background()
	err := client.XGroupCreateMkStream(ctx, StreamKey, GroupName, "0").Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			panic(err)
		}
	}
	return q
}

// CheckAndSetIdempotency atomically claims an idempotency key for jobID.
// If the key was unclaimed, it's now bound to jobID and isNew is true -
// the caller should proceed to enqueue. If the key was already claimed,
// isNew is false and existingID is the job that owns it - the caller
// should hand that back to the client instead of enqueueing again.
func (q *Queue) CheckAndSetIdempotency(ctx context.Context, key, jobID string) (existingID string, isNew bool, err error) {
	redisKey := fmt.Sprintf(idemKeyFmt, key)
	ok, err := q.client.SetNX(ctx, redisKey, jobID, idemTTL).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return jobID, true, nil
	}
	existing, err := q.client.Get(ctx, redisKey).Result()
	if err != nil {
		return "", false, err
	}
	return existing, false, nil
}

// SetJobStatus snapshots the job's current state so GetJob can answer
// status queries without scanning the stream/retry/dlq structures. Callers
// invoke this at every transition - enqueued, picked up, succeeded, failed,
// dead - so it's always the most recent write that wins.
func (q *Queue) SetJobStatus(ctx context.Context, j job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.client.Set(ctx, fmt.Sprintf(jobKeyFmt, j.ID), data, jobTTL).Err()
}
func (q *Queue) GetJob(ctx context.Context, id string) (job.Job, error) {
	data, err := q.client.Get(ctx, fmt.Sprintf(jobKeyFmt, id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return job.Job{}, ErrJobNotFound
	}
	if err != nil {
		return job.Job{}, err
	}
	var j job.Job
	if err := json.Unmarshal(data, &j); err != nil {
		return job.Job{}, err
	}
	return j, nil
}

func (q *Queue) Push(ctx context.Context, j job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.pushRaw(ctx, data)
}

// pushRaw re-enters already-marshalled job bytes onto the stream, used by
// Schedule and the watchdog when requeuing.
func (q *Queue) pushRaw(ctx context.Context, data []byte) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey,
		Values: map[string]any{dataField: data},
	}).Err()
}

// ReadGroup blocks until a job is available for this consumer. The read
// itself atomically moves the entry into the group's pending entries list
// (PEL) - that's the inflight transition, no separate call needed.
func (q *Queue) ReadGroup(ctx context.Context, consumer string) (Msg, error) {
	res, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    GroupName,
		Consumer: consumer,
		Streams:  []string{StreamKey, ">"},
		Count:    1,
		Block:    0,
	}).Result()
	if err != nil {
		return Msg{}, err
	}
	entry := res[0].Messages[0]
	data, _ := entry.Values[dataField].(string)
	return Msg{ID: entry.ID, Data: []byte(data)}, nil
}
func (q *Queue) Ack(ctx context.Context, msgID string) error {
	return q.client.XAck(ctx, StreamKey, GroupName, msgID).Err()
}
func (q *Queue) Fail(ctx context.Context, msgID string, j job.Job) error {
	if err := q.Ack(ctx, msgID); err != nil {
		return err
	}
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
		if err := q.pushRaw(ctx, []byte(j)); err != nil {
			return err
		}
		if err := q.client.ZRem(ctx, RetryKey, j).Err(); err != nil {
			return err
		}
	}
	return nil
}

// InflightCleanup is the watchdog pass: it finds entries claimed by a
// consumer but idle (no ack) past the timeout, claims them onto itself,
// requeues the original job bytes as a fresh entry, and acks off the stale
// one so it drops out of the PEL.
func (q *Queue) InflightCleanup(ctx context.Context, timeout time.Duration) error {
	pending, err := q.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: StreamKey,
		Group:  GroupName,
		Idle:   timeout,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	ids := make([]string, 0, len(pending))
	for _, p := range pending {
		ids = append(ids, p.ID)
	}
	claimed, err := q.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   StreamKey,
		Group:    GroupName,
		Consumer: "watchdog",
		MinIdle:  timeout,
		Messages: ids,
	}).Result()
	if err != nil {
		return err
	}
	for _, entry := range claimed {
		data, _ := entry.Values[dataField].(string)
		if err := q.pushRaw(ctx, []byte(data)); err != nil {
			return err
		}
		if err := q.Ack(ctx, entry.ID); err != nil {
			return err
		}
	}
	return nil
}
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	groups, err := q.client.XInfoGroups(ctx, StreamKey).Result()
	if err != nil {
		return Stats{}, err
	}
	var pending, inflight int64
	for _, g := range groups {
		if g.Name == GroupName {
			pending = g.Lag
			inflight = g.Pending
			break
		}
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
