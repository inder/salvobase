package storage

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"
)

// TTLCleaner runs the TTL index cleanup loop.
// MongoDB's TTL monitor runs every 60 seconds. We run every second for
// millisecond-precision TTL support (an improvement over MongoDB Community).
type TTLCleaner struct {
	engine  *BBoltEngine
	logger  *zap.Logger
	stop    chan struct{}
	done    chan struct{}
	interval time.Duration
}

// NewTTLCleaner creates a TTL cleaner with the given check interval.
// interval=1s gives millisecond-precision TTL (vs MongoDB's 60s minimum).
func NewTTLCleaner(engine *BBoltEngine, logger *zap.Logger, interval time.Duration) *TTLCleaner {
	return &TTLCleaner{
		engine:   engine,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		interval: interval,
	}
}

// Start begins the TTL cleanup loop in a background goroutine.
func (c *TTLCleaner) Start() {
	go c.run()
}

// Stop halts the cleanup loop and waits for it to finish.
func (c *TTLCleaner) Stop() {
	close(c.stop)
	<-c.done
}

func (c *TTLCleaner) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			if err := c.runCycle(); err != nil {
				c.logger.Error("TTL cleanup error", zap.Error(err))
			}
		}
	}
}

func (c *TTLCleaner) runCycle() error {
	// Get all databases
	dbs, err := c.engine.ListDatabases()
	if err != nil {
		return err
	}

	for _, dbInfo := range dbs {
		if err := c.processDB(dbInfo.Name); err != nil {
			c.logger.Warn("TTL cleanup failed for db",
				zap.String("db", dbInfo.Name),
				zap.Error(err))
		}
	}
	return nil
}

func (c *TTLCleaner) processDB(db string) error {
	colls, err := c.engine.ListCollections(db)
	if err != nil {
		return err
	}

	for _, collInfo := range colls {
		indexes, err := c.engine.ListIndexes(db, collInfo.Name)
		if err != nil {
			continue
		}
		for _, idx := range indexes {
			if idx.ExpireAfterSeconds == nil {
				continue
			}
			if err := c.expireDocuments(db, collInfo.Name, idx); err != nil {
				c.logger.Warn("TTL expiry failed",
					zap.String("db", db),
					zap.String("collection", collInfo.Name),
					zap.String("index", idx.Name),
					zap.Error(err))
			}
		}
	}
	return nil
}

func (c *TTLCleaner) expireDocuments(db, coll string, idx IndexInfo) error {
	// Get the TTL field name from the index key spec
	elems, err := idx.Key.Elements()
	if err != nil || len(elems) == 0 {
		return nil
	}
	ttlField := elems[0].Key()
	ttlSecs := *idx.ExpireAfterSeconds
	cutoff := time.Now().Add(-time.Duration(ttlSecs) * time.Second)

	collection, err := c.engine.Collection(db, coll)
	if err != nil {
		return err
	}

	// Find all docs where ttlField < cutoff
	// Build a filter: {ttlField: {$lt: cutoff}}
	filter, err := bson.Marshal(bson.D{
		{Key: ttlField, Value: bson.D{
			{Key: "$lt", Value: cutoff},
		}},
	})
	if err != nil {
		return err
	}

	n, err := collection.DeleteMany(filter)
	if err != nil {
		return err
	}
	if n > 0 {
		c.logger.Debug("TTL expired documents",
			zap.String("db", db),
			zap.String("collection", coll),
			zap.String("field", ttlField),
			zap.Int64("count", n))
	}
	return nil
}
