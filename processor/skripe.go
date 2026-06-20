package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"retsu/job"
	"time"
)

type Func func(job.Job) error

var registry = map[string]Func{}

// Register wires a processor in under a job type. Call it from an init()
// in whichever file owns that job type, or explicitly from main - either
// way it has to happen before workers start reading.
func Register(jobType string, fn Func) {
	registry[jobType] = fn
}

func Process(j job.Job) error {
	fn, ok := registry[j.Type]
	if !ok {
		return fmt.Errorf("no processor registered for job type %q", j.Type)
	}
	return fn(j)
}

func init() {
	Register("payment", processPayment)
}

func processPayment(j job.Job) error {
	var p job.PaymentPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	if rand.Float64() < 0.2 {
		return errors.New("gateways faillllledddd!!!!")
	}
	log.Printf("SUCCES67SS!!")
	return nil
}
