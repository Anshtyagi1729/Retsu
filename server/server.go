package server

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"retsu/job"
	"retsu/queue"
	"time"

	"github.com/google/uuid"
)

//go:embed static
var staticFS embed.FS

//what to do here
// this is the producer of jobs alright
// s0 the jobs are created here and takes paymentdetails in reqbody
// post/queue pushes this job to redis shows 202 processing on the screen alrihgt
// so clients creates a jpob pushes that to this producer which pushes to queue

type Server struct {
	queue      *queue.Queue
	httpServer *http.Server
}

func New(q *queue.Queue) *Server {
	s := &Server{queue: q}
	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue", s.handleEnq)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /jobs/{id}", s.handleJobStatus)
	mux.Handle("GET /", http.FileServerFS(static))
	s.httpServer = &http.Server{Handler: mux}
	return s
}

// EnqueueRequest is the type-agnostic envelope every job goes through.
// Payload is opaque to Retsu - it's handed unchanged to whichever
// processor is registered for Type.
type EnqueueRequest struct {
	Type           string          `json:"type"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CallbackUrl    string          `json:"callback_url,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

func (s *Server) handleEnq(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Type == "" || len(req.Payload) == 0 {
		http.Error(w, "missing required field", http.StatusBadRequest)
		return
	}
	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	j := job.Job{
		ID:             uuid.NewString(),
		Type:           req.Type,
		Status:         job.StatusPending,
		Attempts:       0,
		MaxAttempts:    maxAttempts,
		CreatedAt:      time.Now(),
		RunAt:          time.Now(),
		IdempotencyKey: req.IdempotencyKey,
		CallbackUrl:    req.CallbackUrl,
		Payload:        req.Payload,
	}

	if req.IdempotencyKey != "" {
		existingID, isNew, err := s.queue.CheckAndSetIdempotency(r.Context(), req.IdempotencyKey, j.ID)
		if err != nil {
			http.Error(w, "idempotency check failed", http.StatusInternalServerError)
			return
		}
		if !isNew {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"job_id": existingID})
			return
		}
	}

	if err := s.queue.SetJobStatus(r.Context(), j); err != nil {
		http.Error(w, "cannot record job status", http.StatusInternalServerError)
		return
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
func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.queue.GetJob(r.Context(), id)
	if errors.Is(err, queue.ErrJobNotFound) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "could not get job", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(j)
}
// Start blocks serving HTTP until Shutdown is called (or the listener
// fails outright). It returns http.ErrServerClosed on a clean shutdown -
// callers should treat that as success, not an error to log.
func (s *Server) Start(addr string) error {
	s.httpServer.Addr = addr
	return s.httpServer.ListenAndServe()
}

// Shutdown stops accepting new connections immediately and waits (up to
// ctx's deadline) for in-flight requests to finish before returning.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
