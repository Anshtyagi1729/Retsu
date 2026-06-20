package job

import (
	"encoding/json"
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusInFlight  JobStatus = "in_flight"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusDead      JobStatus = "dead"
)

type Job struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	Status         JobStatus `json:"status"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	CreatedAt      time.Time `json:"created_at"`
	RunAt          time.Time `json:"run_at"`
	LastError      string    `json:"last_error"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	CallbackUrl    string    `json:"callback_url,omitempty"`
	// ths lil hsi is owned by app opaque to Retsu
	Payload json.RawMessage `json:"payload"`
}
type PaymentPayload struct {
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
	Currency  string `json:"currency"`
	CardToken string `json:"card_token"`
}
