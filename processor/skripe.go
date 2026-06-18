package processor

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"retsu/job"
	"time"
)

func Process(j job.Job) error {
	var p job.PaymentPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return err
	}
	// idempotency checks later and allthat shit
	time.Sleep(500 * time.Millisecond)
	if rand.Float64() < 0.1 {
		return errors.New("gateways faillllledddd!!!!ohno!!")
	}
	log.Printf("successsssssfull paymentt!!")
	return nil
}
