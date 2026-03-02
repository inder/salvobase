// Package server implements the MongoDB-compatible TCP server, connection handling,
// Prometheus metrics, and the HTTP/REST API.
package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/inder/mongoclone/internal/auth"
	"github.com/inder/mongoclone/internal/commands"
	"github.com/inder/mongoclone/internal/storage"
)

// Config holds all server configuration options.
type Config struct {
	Port           int
	HTTPPort       int
	BindIP         string
	MaxConnections int
	DataDir        string
	NoAuth         bool
	LogLevel       string
	LogFormat      string
	AuditLog       string
	Compression    string
	SyncOnWrite    bool
	MaxDocSize     int
	RequestsPerSec int
	TLS            bool
	TLSCert        string
	TLSKey         string
	Version        string
	BuildTime      string
}

// Server is the main MongClone server.
type Server struct {
	cfg        Config
	engine     storage.Engine
	authMgr    *auth.Manager
	dispatcher *commands.Dispatcher
	listener   net.Listener
	logger     *zap.Logger

	mu          sync.RWMutex
	connections map[int64]*Connection
	nextConnID  atomic.Int64

	currentConns atomic.Int32
	totalConns   atomic.Int64

	shutdown chan struct{}
	done     chan struct{}

	startTime time.Time
}

// New creates a new Server, initialises the storage engine, auth manager,
// and command dispatcher, and binds the TCP listener.
func New(cfg Config, logger *zap.Logger) (*Server, error) {
	// Create data directory.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("server.New: failed to create data dir %s: %w", cfg.DataDir, err)
	}

	// Initialise storage engine.
	compression := cfg.Compression
	if compression == "" {
		compression = "none"
	}
	engine, err := storage.NewBBoltEngine(cfg.DataDir, compression, cfg.SyncOnWrite)
	if err != nil {
		return nil, fmt.Errorf("server.New: failed to create storage engine: %w", err)
	}

	// Initialise auth manager.
	authMgr := auth.NewManager(engine.Users(), cfg.NoAuth)

	// Initialise command dispatcher.
	dispatcher := commands.NewDispatcher(engine, authMgr, logger)

	// Bind TCP listener.
	addr := fmt.Sprintf("%s:%d", cfg.BindIP, cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		engine.Close()
		return nil, fmt.Errorf("server.New: failed to listen on %s: %w", addr, err)
	}

	s := &Server{
		cfg:         cfg,
		engine:      engine,
		authMgr:     authMgr,
		dispatcher:  dispatcher,
		listener:    listener,
		logger:      logger,
		connections: make(map[int64]*Connection),
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
		startTime:   time.Now(),
	}

	logger.Info("listening for MongoDB connections",
		zap.String("addr", addr),
		zap.Bool("noAuth", cfg.NoAuth),
	)
	return s, nil
}

// Run starts the server accept loop. It blocks until the server is shut down.
func (s *Server) Run() error {
	// Start HTTP server (metrics + REST API) if configured.
	if s.cfg.HTTPPort > 0 {
		go s.startHTTPServer()
	}

	// Start cursor cleanup goroutine.
	go s.cursorCleanupLoop()

	// Accept incoming connections.
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if we've been asked to shut down.
			select {
			case <-s.shutdown:
				close(s.done)
				return nil
			default:
			}
			s.logger.Error("accept error", zap.Error(err))
			continue
		}

		// Enforce max connections limit.
		if s.cfg.MaxConnections > 0 && int(s.currentConns.Load()) >= s.cfg.MaxConnections {
			s.logger.Warn("max connections reached, rejecting new connection",
				zap.Int("max", s.cfg.MaxConnections),
			)
			conn.Close()
			continue
		}

		connID := s.nextConnID.Add(1)
		c := newConnection(connID, conn, s)

		s.mu.Lock()
		s.connections[connID] = c
		s.mu.Unlock()

		s.currentConns.Add(1)
		s.totalConns.Add(1)
		metricConnections.Inc()

		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.connections, connID)
				s.mu.Unlock()
				s.currentConns.Add(-1)
				metricConnections.Dec()
			}()
			c.serve()
		}()
	}
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	s.logger.Info("shutting down server")

	// Signal accept loop to exit.
	close(s.shutdown)
	s.listener.Close()

	// Wait for accept loop to finish (with timeout).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case <-s.done:
	case <-ctx.Done():
		s.logger.Warn("shutdown timed out waiting for accept loop")
	}

	// Close all active connections.
	s.mu.RLock()
	conns := make([]*Connection, 0, len(s.connections))
	for _, c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.RUnlock()

	for _, c := range conns {
		c.close()
	}

	// Close storage engine.
	if err := s.engine.Close(); err != nil {
		s.logger.Error("error closing storage engine", zap.Error(err))
		return err
	}

	s.logger.Info("server shutdown complete")
	return nil
}

// cursorCleanupLoop periodically removes cursors that have been idle too long.
func (s *Server) cursorCleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.engine.Cursors().Cleanup(600) // 600 seconds = 10 minutes
		case <-s.shutdown:
			return
		}
	}
}
