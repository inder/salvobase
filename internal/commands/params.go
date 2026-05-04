package commands

import (
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// RuntimeConfig holds server parameters that can be inspected via getParameter
// and, for a subset, modified at runtime via setParameter.
// All methods are safe for concurrent use.
type RuntimeConfig struct {
	mu sync.RWMutex

	logLevel       string
	maxConnections int
	requestsPerSec int
	syncOnWrite    bool
	compression    string
}

// NewRuntimeConfig creates a RuntimeConfig initialized from server startup settings.
func NewRuntimeConfig(logLevel, compression string, maxConnections, requestsPerSec int, syncOnWrite bool) *RuntimeConfig {
	if logLevel == "" {
		logLevel = "info"
	}
	if compression == "" {
		compression = "none"
	}
	return &RuntimeConfig{
		logLevel:       logLevel,
		maxConnections: maxConnections,
		requestsPerSec: requestsPerSec,
		syncOnWrite:    syncOnWrite,
		compression:    compression,
	}
}

func (r *RuntimeConfig) GetLogLevel() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.logLevel
}

func (r *RuntimeConfig) SetLogLevel(v string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logLevel = v
}

func (r *RuntimeConfig) GetMaxConnections() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.maxConnections
}

func (r *RuntimeConfig) GetRequestsPerSec() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.requestsPerSec
}

func (r *RuntimeConfig) SetRequestsPerSec(v int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestsPerSec = v
}

func (r *RuntimeConfig) GetSyncOnWrite() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.syncOnWrite
}

func (r *RuntimeConfig) GetCompression() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.compression
}

// supportedGetParams is the complete set of parameters supported by getParameter.
var supportedGetParams = map[string]struct{}{
	"logLevel":       {},
	"maxConnections": {},
	"requestsPerSec": {},
	"syncOnWrite":    {},
	"compression":    {},
}

// handleGetParameter handles the "getParameter" command.
//
// Usage:
//
//	{ getParameter: 1, logLevel: 1 }          — fetch a single parameter
//	{ getParameter: 1, logLevel: 1, ... }     — fetch multiple parameters
//	{ getParameter: "*" }                     — fetch all supported parameters
func handleGetParameter(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	if ctx.RuntimeCfg == nil {
		return nil, storage.Errorf(storage.ErrCodeCommandFailed, "getParameter: runtime config not available")
	}

	getParamVal, err := cmd.LookupErr("getParameter")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "getParameter: missing getParameter field")
	}

	// Check for the "return all" wildcard.
	requestAll := false
	if getParamVal.Type == bson.TypeString {
		if s, ok := getParamVal.StringValueOK(); ok && s == "*" {
			requestAll = true
		}
	}

	cfg := ctx.RuntimeCfg
	result := bson.D{}

	if requestAll {
		result = append(result,
			bson.E{Key: "logLevel", Value: cfg.GetLogLevel()},
			bson.E{Key: "maxConnections", Value: int32(cfg.GetMaxConnections())},
			bson.E{Key: "requestsPerSec", Value: int32(cfg.GetRequestsPerSec())},
			bson.E{Key: "syncOnWrite", Value: cfg.GetSyncOnWrite()},
			bson.E{Key: "compression", Value: cfg.GetCompression()},
		)
	} else {
		elems, _ := cmd.Elements()
		for _, elem := range elems {
			key := elem.Key()
			// Skip the command sentinel and standard wire-protocol fields.
			switch key {
			case "getParameter", "$db", "lsid", "$clusterTime", "$readPreference":
				continue
			}
			if _, ok := supportedGetParams[key]; !ok {
				return nil, storage.Errorf(storage.ErrCodeBadValue,
					"getParameter: unknown parameter: %s", key)
			}
			switch key {
			case "logLevel":
				result = append(result, bson.E{Key: "logLevel", Value: cfg.GetLogLevel()})
			case "maxConnections":
				result = append(result, bson.E{Key: "maxConnections", Value: int32(cfg.GetMaxConnections())})
			case "requestsPerSec":
				result = append(result, bson.E{Key: "requestsPerSec", Value: int32(cfg.GetRequestsPerSec())})
			case "syncOnWrite":
				result = append(result, bson.E{Key: "syncOnWrite", Value: cfg.GetSyncOnWrite()})
			case "compression":
				result = append(result, bson.E{Key: "compression", Value: cfg.GetCompression()})
			}
		}
	}

	result = append(result, bson.E{Key: "ok", Value: float64(1)})
	return marshalResponse(ctx, result), nil
}

// handleSetParameter handles the "setParameter" command.
//
// Mutable parameters: logLevel, requestsPerSec.
// Read-only parameters (maxConnections, syncOnWrite, compression) are not settable
// at runtime and return an error if specified.
func handleSetParameter(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	if ctx.RuntimeCfg == nil {
		return nil, storage.Errorf(storage.ErrCodeCommandFailed, "setParameter: runtime config not available")
	}

	elems, err := cmd.Elements()
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "setParameter: invalid command document")
	}

	cfg := ctx.RuntimeCfg
	changed := false

	for _, elem := range elems {
		key := elem.Key()
		// Skip the command sentinel and standard wire-protocol fields.
		switch key {
		case "setParameter", "$db", "lsid", "$clusterTime", "$readPreference":
			continue
		}

		switch key {
		case "logLevel":
			val := elem.Value()
			s, ok := val.StringValueOK()
			if !ok {
				return nil, storage.Errorf(storage.ErrCodeBadValue,
					"setParameter: logLevel must be a string")
			}
			// Normalise "warning" -> "warn" for consistency with zap.
			if s == "warning" {
				s = "warn"
			}
			switch s {
			case "debug", "info", "warn", "error":
			default:
				return nil, storage.Errorf(storage.ErrCodeBadValue,
					"setParameter: invalid logLevel %q; must be one of: debug, info, warn, error", s)
			}
			cfg.SetLogLevel(s)
			changed = true

		case "requestsPerSec":
			val := elem.Value()
			var v int64
			switch val.Type {
			case bson.TypeInt32:
				v = int64(val.Int32())
			case bson.TypeInt64:
				v = val.Int64()
			case bson.TypeDouble:
				v = int64(val.Double())
			default:
				return nil, storage.Errorf(storage.ErrCodeBadValue,
					"setParameter: requestsPerSec must be a number")
			}
			if v < 0 {
				return nil, storage.Errorf(storage.ErrCodeBadValue,
					"setParameter: requestsPerSec must be >= 0")
			}
			cfg.SetRequestsPerSec(int(v))
			changed = true

		default:
			return nil, storage.Errorf(storage.ErrCodeBadValue,
				"setParameter: unknown or read-only parameter: %s", key)
		}
	}

	if !changed {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "setParameter: no parameters specified")
	}

	return BuildOKResponse(), nil
}
