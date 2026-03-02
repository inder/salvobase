package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// cursorStore manages long-lived server-side cursors for getMore requests.
type cursorStore struct {
	mu      sync.RWMutex
	cursors map[int64]*cursorEntry
	nextID  atomic.Int64
}

type cursorEntry struct {
	cursor    Cursor
	createdAt time.Time
	lastUsed  time.Time
}

// Register assigns a new cursor ID, stores the cursor, and returns the ID.
// The returned ID can be used by clients for getMore requests.
func (s *cursorStore) Register(c Cursor) int64 {
	id := s.nextID.Add(1)

	// Update the cursor's internal id if it's a sliceCursor
	if sc, ok := c.(*sliceCursor); ok {
		sc.mu.Lock()
		sc.id = id
		sc.mu.Unlock()
	}

	now := time.Now()
	s.mu.Lock()
	s.cursors[id] = &cursorEntry{
		cursor:    c,
		createdAt: now,
		lastUsed:  now,
	}
	s.mu.Unlock()
	return id
}

// Get retrieves a cursor by ID and updates its last-used timestamp.
// Returns (cursor, true) if found, (nil, false) otherwise.
func (s *cursorStore) Get(id int64) (Cursor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cursors[id]
	if !ok {
		return nil, false
	}
	entry.lastUsed = time.Now()
	return entry.cursor, true
}

// Delete closes and removes a cursor by ID. No-op if the cursor doesn't exist.
func (s *cursorStore) Delete(id int64) {
	s.mu.Lock()
	entry, ok := s.cursors[id]
	if ok {
		delete(s.cursors, id)
	}
	s.mu.Unlock()
	if ok {
		entry.cursor.Close() //nolint:errcheck
	}
}

// DeleteMany closes and removes multiple cursors.
func (s *cursorStore) DeleteMany(ids []int64) {
	for _, id := range ids {
		s.Delete(id)
	}
}

// Cleanup removes cursors that have been idle longer than maxIdleSecs seconds.
func (s *cursorStore) Cleanup(maxIdleSecs int) {
	threshold := time.Duration(maxIdleSecs) * time.Second
	now := time.Now()

	s.mu.Lock()
	var toClose []Cursor
	for id, entry := range s.cursors {
		if now.Sub(entry.lastUsed) > threshold {
			toClose = append(toClose, entry.cursor)
			delete(s.cursors, id)
		}
	}
	s.mu.Unlock()

	for _, c := range toClose {
		c.Close() //nolint:errcheck
	}
}
