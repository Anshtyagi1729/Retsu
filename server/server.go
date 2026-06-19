package server

import (
	"encoding/json"
	"net/http"
	"retsu/job"
	"retsu/queue"
	"time"

	"github.com/google/uuid"
)

//what to do here
// this is the producer of jobs alright
// s0 the jobs are created here and takes paymentdetails in reqbody
// post/queue pushes this job to redis shows 202 processing on the screen alrihgt
// so clients creates a jpob pushes that to this producer which pushes to queue

type Server struct {
	queue *queue.Queue
}

func New(q *queue.Queue) *Server {
	return &Server{queue: q}
}
func (s *Server) handleEnq(w http.ResponseWriter, r *http.Request) {
	var p job.PaymentPayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if p.Amount <= 0 || p.CardToken == "" {
		http.Error(w, "missing required field", http.StatusBadRequest)
		return
	}
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		http.Error(w, "cant turn to bytes", http.StatusInternalServerError)
		return
	}
	j := job.Job{
		ID:          uuid.NewString(),
		Type:        "payment",
		Status:      job.StatusPending,
		Attempts:    0,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
		RunAt:       time.Now(),
		Payload:     payloadBytes,
	}
	if err := s.queue.Push(r.Context(), j); err != nil {
		http.Error(w, "cannot push to queue", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"job_id": j.ID})
}
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.queue.Stats(r.Context())
	if err != nil {
		http.Error(w, "could not get stats", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(stats)
}
func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue", s.handleEnq)
	mux.HandleFunc("GET /stats", s.handleStats)
	return http.ListenAndServe(addr, mux)
}
