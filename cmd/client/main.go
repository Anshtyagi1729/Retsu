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
			UserID:    uuid.NewString(),
			Amount:    1099,
			Currency:  "usd",
			CardToken: uuid.NewString(),
		}
		payloadBytes, err := json.Marshal(p)
		if err != nil {
			log.Printf("error encoding:%v", err)
			continue
		}
		req := struct {
			Type           string          `json:"type"`
			IdempotencyKey string          `json:"idempotency_key"`
			CallbackUrl    string          `json:"callback_url"`
			Payload        json.RawMessage `json:"payload"`
		}{
			Type:           "payment",
			IdempotencyKey: uuid.NewString(),
			CallbackUrl:    "http://localhost:9000/hook",
			Payload:        payloadBytes,
		}
		reqBytes, err := json.Marshal(req)
		if err != nil {
			log.Printf("error encoding envelope:%v", err)
			continue
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(reqBytes))
		if err != nil {
			log.Printf("encode failed: %v", err)
			continue
		}
		resp.Body.Close()
		log.Printf("sent->%s", resp.Status)
	}
}
