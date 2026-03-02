package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"

	"github.com/inder/mongoclone/internal/commands"
)

// ─── Prometheus metrics ───────────────────────────────────────────────────────

var (
	metricConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mongoclone_connections_active",
		Help: "Number of active client connections",
	})

	metricOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mongoclone_operations_total",
		Help: "Total number of operations by type",
	}, []string{"op"})

	metricCommandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mongoclone_command_duration_seconds",
		Help:    "Command execution duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"command"})

	metricDocumentsInserted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mongoclone_documents_inserted_total",
		Help: "Documents inserted",
	}, []string{"db", "collection"})

	metricDocumentsQueried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mongoclone_documents_queried_total",
		Help: "Documents returned by queries",
	}, []string{"db", "collection"})

	metricErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mongoclone_errors_total",
		Help: "Total command errors by type",
	}, []string{"command"})

	metricBytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mongoclone_bytes_sent_total",
		Help: "Total bytes sent to clients",
	})

	metricBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mongoclone_bytes_received_total",
		Help: "Total bytes received from clients",
	})
)

// startHTTPServer starts the Prometheus + REST API HTTP server.
func (s *Server) startHTTPServer() {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint.
	mux.Handle("/metrics", promhttp.Handler())

	// Health check.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":1,"status":"healthy"}`))
	})

	// REST API.
	s.registerRESTAPI(mux)

	addr := fmt.Sprintf("%s:%d", s.cfg.BindIP, s.cfg.HTTPPort)
	s.logger.Info("starting HTTP server", zap.String("addr", addr))
	httpSrv := &http.Server{Addr: addr, Handler: mux}
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.logger.Error("HTTP server error", zap.Error(err))
	}
}

// registerRESTAPI registers simple JSON REST endpoints on the given mux.
// POST /api/v1/db/<db>/collection/<coll>/find
// POST /api/v1/db/<db>/collection/<coll>/insert
// POST /api/v1/db/<db>/collection/<coll>/update
// POST /api/v1/db/<db>/collection/<coll>/delete
// POST /api/v1/db/<db>/collection/<coll>/aggregate
func (s *Server) registerRESTAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/", s.handleRESTRequest)
}

// handleRESTRequest dispatches REST API requests to the MongoDB command dispatcher.
// Path format: /api/v1/db/<dbname>/collection/<collname>/<command>
func (s *Server) handleRESTRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"ok":0,"errmsg":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Parse path: /api/v1/db/<db>/collection/<coll>/<cmd>
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/")
	parts := strings.Split(path, "/")

	// Expected: ["db", "<dbname>", "collection", "<collname>", "<cmd>"]
	if len(parts) != 5 || parts[0] != "db" || parts[2] != "collection" {
		http.Error(w, `{"ok":0,"errmsg":"invalid path"}`, http.StatusBadRequest)
		return
	}

	dbName := parts[1]
	collName := parts[3]
	cmdStr := parts[4]

	if dbName == "" || collName == "" || cmdStr == "" {
		http.Error(w, `{"ok":0,"errmsg":"missing db, collection, or command in path"}`, http.StatusBadRequest)
		return
	}

	// Read request body as JSON and convert to BSON.
	var bodyMap map[string]interface{}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&bodyMap); err != nil {
			http.Error(w, fmt.Sprintf(`{"ok":0,"errmsg":"invalid JSON body: %v"}`, err), http.StatusBadRequest)
			return
		}
	}

	// Build the command document.
	cmdD := bson.D{{cmdStr, collName}}
	for k, v := range bodyMap {
		cmdD = append(cmdD, bson.E{Key: k, Value: v})
	}

	cmdRaw, err := bson.Marshal(cmdD)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"ok":0,"errmsg":"failed to marshal command: %v"}`, err), http.StatusInternalServerError)
		return
	}

	ctx := &commands.Context{
		DB:         dbName,
		Engine:     s.engine,
		Auth:       s.authMgr,
		Logger:     s.logger,
		ConnID:     -1, // REST API uses a virtual connection ID
		NoAuth:     s.cfg.NoAuth,
		RemoteAddr: r.RemoteAddr,
	}

	resp := s.dispatcher.Dispatch(ctx, cmdRaw)

	// Convert BSON response to extended JSON.
	jsonBytes, err := bson.MarshalExtJSON(resp, false, false)
	if err != nil {
		http.Error(w, `{"ok":0,"errmsg":"internal error encoding response"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Determine HTTP status from the "ok" field.
	statusCode := http.StatusOK
	if okVal, lookupErr := resp.LookupErr("ok"); lookupErr == nil {
		switch okVal.Type {
		case bson.TypeDouble:
			if okVal.Double() == 0 {
				statusCode = http.StatusBadRequest
			}
		case bson.TypeInt32:
			if okVal.Int32() == 0 {
				statusCode = http.StatusBadRequest
			}
		}
	}
	w.WriteHeader(statusCode)
	w.Write(jsonBytes)
}

// recordCommandMetrics records Prometheus metrics for a completed command.
// db and coll may be empty for non-collection commands.
func recordCommandMetrics(cmdName string, db, coll string, docCount int, isError bool) {
	metricOpsTotal.WithLabelValues(cmdName).Inc()
	if isError {
		metricErrors.WithLabelValues(cmdName).Inc()
	}
	switch cmdName {
	case "insert":
		if docCount > 0 && db != "" && coll != "" {
			metricDocumentsInserted.WithLabelValues(db, coll).Add(float64(docCount))
		}
	case "find", "aggregate":
		if docCount > 0 && db != "" && coll != "" {
			metricDocumentsQueried.WithLabelValues(db, coll).Add(float64(docCount))
		}
	}
}

