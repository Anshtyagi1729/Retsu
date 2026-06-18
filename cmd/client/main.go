package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"retsu/job"

	"github.com/google/uuid"
)

const url = "http://localhost:8080/queue"

func main() {
	for range 20 {
		p := job.PaymentPayload{
			UserID:         uuid.NewString(),
			Amount:         1099,
			Currency:       "usd",
			IdempotencyKey: uuid.NewString(),
			CardToken:      uuid.NewString(),
			CallbackUrl:    "http://localhost:9000/hook",
		}
		payloadBytes, err := json.Marshal(p)
		if err != nil {
			log.Printf("error encoding:%v", err)
			continue
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(payloadBytes))
		if err != nil {
			log.Printf("encode failed: %v", err)
			continue
		}
		resp.Body.Close()
		log.Printf("sent->%s", resp.Status)
	}
}
