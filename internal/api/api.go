// Package api exposes the broker over HTTP with JSON bodies.
//
// Error responses are JSON: {"error":"..."} with an appropriate status code.
// Publish bodies are capped at 2 MiB before JSON decoding; record size
// limits are enforced by the log (1 MiB key+value).
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/anthonychiu/spool/internal/broker"
	spoollog "github.com/anthonychiu/spool/internal/log"
)

const (
	maxPublishBody = 2 << 20
	defaultLimit   = 100
	maxLimit       = 1000
)

// Server routes HTTP requests to a broker.
type Server struct {
	broker *broker.Broker
	mux    *http.ServeMux
}

func New(b *broker.Broker) *Server {
	s := &Server{broker: b, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /streams/{stream}/records", s.publish)
	s.mux.HandleFunc("GET /streams/{stream}/partitions/{partition}/records", s.read)
	s.mux.HandleFunc("GET /groups/{group}/streams/{stream}/partitions/{partition}/offset", s.getOffset)
	s.mux.HandleFunc("PUT /groups/{group}/streams/{stream}/partitions/{partition}/offset", s.putOffset)
	s.mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type apiRecord struct {
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

func toAPIRecord(r spoollog.Record) apiRecord {
	return apiRecord{Offset: r.Offset, Timestamp: r.Timestamp, Key: string(r.Key), Value: string(r.Value)}
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPublishBody))
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if body.Value == "" {
		writeError(w, http.StatusBadRequest, "value is required")
		return
	}
	var key []byte
	if body.Key != "" {
		key = []byte(body.Key)
	}
	partition, offset, err := s.broker.Publish(r.PathValue("stream"), key, []byte(body.Value))
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"partition": partition, "offset": offset})
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	partition, ok := parsePartition(w, r)
	if !ok {
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
		return
	}
	limit := defaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > maxLimit {
			writeError(w, http.StatusBadRequest, "limit must be 1..1000")
			return
		}
	}
	records, err := s.broker.Read(r.PathValue("stream"), partition, offset, limit)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	out := make([]apiRecord, len(records))
	next := offset
	for i, rec := range records {
		out[i] = toAPIRecord(rec)
		next = rec.Offset + 1
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": out, "next_offset": next})
}

func (s *Server) getOffset(w http.ResponseWriter, r *http.Request) {
	partition, ok := parsePartition(w, r)
	if !ok {
		return
	}
	next, err := s.broker.CommittedOffset(r.PathValue("group"), r.PathValue("stream"), partition)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"next_offset": next})
}

func (s *Server) putOffset(w http.ResponseWriter, r *http.Request) {
	partition, ok := parsePartition(w, r)
	if !ok {
		return
	}
	var body struct {
		NextOffset *int64 `json:"next_offset"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	if err := dec.Decode(&body); err != nil || body.NextOffset == nil || *body.NextOffset < 0 {
		writeError(w, http.StatusBadRequest, "next_offset must be a non-negative integer")
		return
	}
	err := s.broker.CommitOffset(r.PathValue("group"), r.PathValue("stream"), partition, *body.NextOffset)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"next_offset": *body.NextOffset})
}

func parsePartition(w http.ResponseWriter, r *http.Request) (int, bool) {
	partition, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil || partition < 0 {
		writeError(w, http.StatusBadRequest, "partition must be a non-negative integer")
		return 0, false
	}
	return partition, true
}

func writeBrokerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, broker.ErrInvalidStream):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, broker.ErrNoSuchStream), errors.Is(err, broker.ErrNoSuchPart):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, spoollog.ErrRecordTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
