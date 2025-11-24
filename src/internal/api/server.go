package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Jonatan852/distributed-query-processing/internal/distributed"
	"github.com/Jonatan852/distributed-query-processing/internal/parser"
	"github.com/Jonatan852/distributed-query-processing/internal/planner"
	"github.com/Jonatan852/distributed-query-processing/internal/runtime/runner"
	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/internal/visualizer"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// ParserFunc permite injetar implementações diferentes do parser SQL.
type ParserFunc func(string) (*query.SelectStatement, error)

// Config define as dependências mínimas do servidor HTTP.
type Config struct {
	Addr         string
	Engine       *storage.Engine
	Planner      *planner.Planner
	Coordinator  *distributed.Coordinator
	Runner       *runner.Runner
	ParseSQL     ParserFunc
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Server expõe API REST para consultas, carga de dados e workers.
type Server struct {
	cfg        Config
	httpServer *http.Server

	workersMu sync.Mutex
	workers   map[string]*workerBridge

	resultsMu sync.RWMutex
	results   map[string]queryResult
}

// NewServer cria o servidor HTTP e registra as rotas.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Engine == nil || cfg.Planner == nil || cfg.Coordinator == nil {
		return nil, fmt.Errorf("engine, planner e coordinator são obrigatórios")
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	s := &Server{
		cfg:     cfg,
		workers: map[string]*workerBridge{},
		results: map[string]queryResult{},
	}
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	return s, nil
}

// Start inicia o servidor HTTP (chamada bloqueante).
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown encerra o servidor com contexto.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/query", s.handleQuery)
	mux.HandleFunc("/query/", s.handleQueryPath)
	mux.HandleFunc("/data/load", s.handleDataLoad)
	mux.HandleFunc("/workers/register", s.handleWorkerRegister)
	mux.HandleFunc("/workers/", s.handleWorkerPath)
	mux.HandleFunc("/swagger", s.handleSwaggerUI)
	mux.HandleFunc("/swagger/openapi.yaml", s.handleSwaggerSpec)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "payload inválido")
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeError(w, http.StatusBadRequest, "sql é obrigatório")
		return
	}
	stmt, err := s.parseSQL(req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("erro ao parsear SQL: %v", err))
		return
	}
	plan, err := s.cfg.Planner.Build(stmt)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("erro no planner: %v", err))
		return
	}
	id, err := s.cfg.Coordinator.Submit(plan)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if s.cfg.Runner != nil {
		go s.executeLocalResult(id, stmt)
	}
	status, _ := s.cfg.Coordinator.QueryStatus(id)
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"id":        id,
		"status":    status,
		"plan_root": plan.Root.Type,
	})
}

func (s *Server) handleQueryPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/query/")
	if path == "" {
		writeError(w, http.StatusNotFound, "query id inválido")
		return
	}
	parts := strings.Split(path, "/")
	id := parts[0]
	if len(parts) == 1 {
		s.handleQueryStatus(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "tree" {
		s.handleQueryTree(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "rota inválida")
}

func (s *Server) handleQueryStatus(w http.ResponseWriter, r *http.Request, id string) {
	status, err := s.cfg.Coordinator.QueryStatus(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	results, _ := s.cfg.Coordinator.QueryResults(id)
	resp := map[string]interface{}{
		"id":      id,
		"status":  status,
		"results": results,
	}
	if res, ok := s.resultFor(id); ok && res.Ready {
		if res.Error != "" {
			resp["result_error"] = res.Error
		} else {
			resp["rows"] = res.Rows
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleQueryTree(w http.ResponseWriter, r *http.Request, id string) {
	plan, err := s.cfg.Coordinator.QueryPlan(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	format := r.URL.Query().Get("format")
	if format == "" || strings.EqualFold(format, "json") {
		data, err := visualizer.PlanToJSON(plan)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}
	if strings.EqualFold(format, "dot") {
		dot, err := visualizer.PlanToDOT(plan)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(dot))
		return
	}
	writeError(w, http.StatusBadRequest, "format deve ser json ou dot")
}

func (s *Server) handleDataLoad(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	var req loadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "payload inválido")
		return
	}
	if err := s.applyLoadRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"table":        req.Table,
		"partition_id": req.PartitionID,
		"status":       "carregado",
	})
}

func (s *Server) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if strings.TrimSpace(req.ID) == "" {
		req.ID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	bridge := newWorkerBridge(req.ID, 30*time.Second)
	s.workersMu.Lock()
	if _, exists := s.workers[bridge.id]; exists {
		s.workersMu.Unlock()
		writeError(w, http.StatusConflict, "worker já registrado")
		return
	}
	s.workers[bridge.id] = bridge
	s.workersMu.Unlock()
	s.cfg.Coordinator.Register(bridge)

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":        bridge.id,
		"secret":    bridge.secret,
		"poll":      fmt.Sprintf("/workers/%s/poll", bridge.id),
		"result":    fmt.Sprintf("/workers/%s/result", bridge.id),
		"heartbeat": fmt.Sprintf("/workers/%s/heartbeat", bridge.id),
	})
}

func (s *Server) handleWorkerPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/workers/")
	if path == "" {
		writeError(w, http.StatusNotFound, "worker id inválido")
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		writeError(w, http.StatusNotFound, "rota inválida")
		return
	}
	id, action := parts[0], parts[1]
	bridge, ok := s.getWorker(id)
	if !ok {
		writeError(w, http.StatusNotFound, "worker não encontrado")
		return
	}
	switch action {
	case "poll":
		s.handleWorkerPoll(w, r, bridge)
	case "result":
		s.handleWorkerResult(w, r, bridge)
	case "heartbeat":
		s.handleWorkerHeartbeat(w, r, bridge)
	default:
		writeError(w, http.StatusNotFound, "rota inválida")
	}
}

func (s *Server) handleWorkerPoll(w http.ResponseWriter, r *http.Request, bridge *workerBridge) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	if !s.authorizeWorker(w, r, bridge) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	task, ok := bridge.waitTask(ctx)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, map[string]distributed.TaskRequest{"task": task})
}

func (s *Server) handleWorkerResult(w http.ResponseWriter, r *http.Request, bridge *workerBridge) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	if !s.authorizeWorker(w, r, bridge) {
		return
	}
	var result distributed.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		writeError(w, http.StatusBadRequest, "payload inválido")
		return
	}
	result.WorkerID = bridge.id
	if !bridge.deliverResult(result) {
		writeError(w, http.StatusConflict, "nenhum task aguardando resultado")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recebido"})
}

func (s *Server) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request, bridge *workerBridge) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "método não suportado")
		return
	}
	if !s.authorizeWorker(w, r, bridge) {
		return
	}
	bridge.updateHeartbeat()
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (s *Server) parseSQL(sql string) (*query.SelectStatement, error) {
	if s.cfg.ParseSQL != nil {
		return s.cfg.ParseSQL(sql)
	}
	return parser.Parse(sql)
}

func (s *Server) getWorker(id string) (*workerBridge, bool) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	bridge, ok := s.workers[id]
	return bridge, ok
}

func (s *Server) authorizeWorker(w http.ResponseWriter, r *http.Request, bridge *workerBridge) bool {
	secret := r.Header.Get("X-Worker-Secret")
	if secret == "" {
		writeError(w, http.StatusUnauthorized, "header X-Worker-Secret obrigatório")
		return false
	}
	if !bridge.validateSecret(secret) {
		writeError(w, http.StatusForbidden, "segredo inválido")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) executeLocalResult(id string, stmt *query.SelectStatement) {
	if s.cfg.Runner == nil {
		return
	}
	rows, err := s.cfg.Runner.Execute(stmt)
	s.storeResult(id, rows, err)
}

func (s *Server) storeResult(id string, rows []map[string]interface{}, execErr error) {
	res := queryResult{Ready: true}
	if execErr != nil {
		res.Error = execErr.Error()
	} else {
		res.Rows = rows
	}
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()
	s.results[id] = res
}

func (s *Server) resultFor(id string) (queryResult, bool) {
	s.resultsMu.RLock()
	defer s.resultsMu.RUnlock()
	res, ok := s.results[id]
	return res, ok
}

type queryResult struct {
	Rows  []map[string]interface{}
	Error string
	Ready bool
}
