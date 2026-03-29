package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

// SqlResponse mirrors the data API response format.
type SqlResponse struct {
	Rows [][]any `json:"rows"`
}

type server struct {
	mu       sync.RWMutex
	response SqlResponse
}

func main() {
	s := &server{}

	// Load initial response from config file if provided.
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/etc/mock/validators.json"
	}
	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, &s.response); err != nil {
			log.Fatalf("Failed to parse config file %s: %v", configPath, err)
		}
		log.Printf("Loaded %d rows from %s", len(s.response.Rows), configPath)
	} else {
		log.Printf("No config file at %s, starting with empty response", configPath)
	}

	http.HandleFunc("/api/sql/query", s.handleQuery)
	http.HandleFunc("/config", s.handleConfig)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	addr := ":8080"
	log.Printf("Data API mock listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Log the query for debugging.
	body, _ := io.ReadAll(r.Body)
	log.Printf("Query: %s", string(body))

	s.mu.RLock()
	defer s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.response)
}

func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var resp SqlResponse
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.response = resp
	s.mu.Unlock()

	log.Printf("Config updated: %d rows", len(resp.Rows))
	w.WriteHeader(http.StatusOK)
}
