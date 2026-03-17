package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/inder/salvobase/internal/auth"
	"github.com/inder/salvobase/internal/commands"
	"github.com/inder/salvobase/internal/storage"
)

// newTestServer creates a minimal Server suitable for REST API unit tests.
// It uses a BBolt engine backed by a temp directory and runs with noAuth.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()

	engine, err := storage.NewBBoltEngine(dir, "none", false)
	if err != nil {
		t.Fatalf("NewBBoltEngine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	logger := zap.NewNop()
	authMgr := auth.NewManager(engine.Users(), true /* noAuth */)
	dispatcher := commands.NewDispatcher(engine, authMgr, logger)

	return &Server{
		cfg:         Config{NoAuth: true},
		engine:      engine,
		authMgr:     authMgr,
		dispatcher:  dispatcher,
		logger:      logger,
		connections: make(map[int64]*Connection),
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
		startTime:   time.Now(),
	}
}

// newRESTHandler registers the REST routes and returns an httptest.Server.
func newRESTHandler(t *testing.T, srv *Server) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	srv.registerRESTAPI(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

// postJSON makes a POST request with a JSON body and returns the decoded response map.
func postJSON(t *testing.T, url string, body interface{}) map[string]interface{} {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return result
}

// TestRESTAggregate_MatchAndGroup verifies the aggregate endpoint executes a
// $match + $group pipeline and returns the expected results.
func TestRESTAggregate_MatchAndGroup(t *testing.T) {
	srv := newTestServer(t)
	ts := newRESTHandler(t, srv)

	base := ts.URL + "/api/v1/db/testdb/collection/items"

	// Insert test documents.
	insertResp := postJSON(t, base+"/insert", map[string]interface{}{
		"documents": []interface{}{
			map[string]interface{}{"status": "active", "category": "A"},
			map[string]interface{}{"status": "active", "category": "A"},
			map[string]interface{}{"status": "inactive", "category": "B"},
			map[string]interface{}{"status": "active", "category": "B"},
		},
	})
	if ok, _ := insertResp["ok"].(float64); ok != 1 {
		t.Fatalf("insert failed: %v", insertResp)
	}

	// Run aggregate: match active docs, group by category, count.
	aggResp := postJSON(t, base+"/aggregate", map[string]interface{}{
		"pipeline": []interface{}{
			map[string]interface{}{"$match": map[string]interface{}{"status": "active"}},
			map[string]interface{}{"$group": map[string]interface{}{
				"_id":   "$category",
				"count": map[string]interface{}{"$sum": 1},
			}},
		},
	})

	if ok, _ := aggResp["ok"].(float64); ok != 1 {
		t.Fatalf("aggregate failed: %v", aggResp)
	}

	cursorDoc, ok := aggResp["cursor"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected cursor in response, got: %v", aggResp)
	}

	firstBatch, ok := cursorDoc["firstBatch"].([]interface{})
	if !ok {
		t.Fatalf("expected firstBatch array, got: %v", cursorDoc)
	}

	// We expect two groups: "A" (count=2) and "B" (count=1).
	if len(firstBatch) != 2 {
		t.Errorf("expected 2 groups, got %d: %v", len(firstBatch), firstBatch)
	}

	counts := make(map[string]float64)
	for _, item := range firstBatch {
		doc, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("unexpected item type %T", item)
		}
		id, _ := doc["_id"].(string)
		count, _ := doc["count"].(float64)
		counts[id] = count
	}

	if counts["A"] != 2 {
		t.Errorf("expected category A count=2, got %v", counts["A"])
	}
	if counts["B"] != 1 {
		t.Errorf("expected category B count=1, got %v", counts["B"])
	}
}

// TestRESTAggregate_EmptyPipeline verifies that an empty pipeline returns all documents.
func TestRESTAggregate_EmptyPipeline(t *testing.T) {
	srv := newTestServer(t)
	ts := newRESTHandler(t, srv)

	base := ts.URL + "/api/v1/db/testdb/collection/docs"

	// Insert 3 documents.
	insertResp := postJSON(t, base+"/insert", map[string]interface{}{
		"documents": []interface{}{
			map[string]interface{}{"n": 1},
			map[string]interface{}{"n": 2},
			map[string]interface{}{"n": 3},
		},
	})
	if ok, _ := insertResp["ok"].(float64); ok != 1 {
		t.Fatalf("insert failed: %v", insertResp)
	}

	aggResp := postJSON(t, base+"/aggregate", map[string]interface{}{
		"pipeline": []interface{}{},
	})

	if ok, _ := aggResp["ok"].(float64); ok != 1 {
		t.Fatalf("aggregate failed: %v", aggResp)
	}

	cursorDoc, ok := aggResp["cursor"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected cursor, got: %v", aggResp)
	}
	firstBatch, ok := cursorDoc["firstBatch"].([]interface{})
	if !ok {
		t.Fatalf("expected firstBatch, got: %v", cursorDoc)
	}
	if len(firstBatch) != 3 {
		t.Errorf("expected 3 docs, got %d", len(firstBatch))
	}
}

// TestRESTAggregate_MissingPipeline verifies that a missing pipeline field returns an error.
func TestRESTAggregate_MissingPipeline(t *testing.T) {
	srv := newTestServer(t)
	ts := newRESTHandler(t, srv)

	url := ts.URL + "/api/v1/db/testdb/collection/docs/aggregate"
	body := `{}`
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ok, _ := result["ok"].(float64); ok != 0 {
		t.Errorf("expected ok=0 for missing pipeline, got %v", result)
	}
}

// TestRESTAggregate_MethodNotAllowed verifies that GET requests are rejected.
func TestRESTAggregate_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)
	ts := newRESTHandler(t, srv)

	resp, err := http.Get(ts.URL + "/api/v1/db/testdb/collection/docs/aggregate")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

// TestRESTAggregate_InvalidPath verifies that malformed paths return 400.
func TestRESTAggregate_InvalidPath(t *testing.T) {
	srv := newTestServer(t)
	ts := newRESTHandler(t, srv)

	resp, err := http.Post(ts.URL+"/api/v1/badpath", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}
