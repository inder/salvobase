package server

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/inder/salvobase/internal/auth"
	"github.com/inder/salvobase/internal/storage"
)

// CreateAdminUser creates an admin user directly in the storage layer.
// Used by the "salvobase admin create-user" CLI subcommand.
// This bypasses the running server and writes directly to the database files.
func CreateAdminUser(dataDir, username, password string) error {
	engine, err := storage.NewBBoltEngine(dataDir, "none", true)
	if err != nil {
		return fmt.Errorf("CreateAdminUser: failed to open storage: %w", err)
	}
	defer engine.Close()

	storedKey, serverKey, salt, iterCount, err := auth.HashPassword(password)
	if err != nil {
		return fmt.Errorf("CreateAdminUser: failed to hash password: %w", err)
	}

	user := storage.User{
		ID:        uuid.New().String(),
		DB:        "admin",
		Username:  username,
		StoredKey: storedKey,
		ServerKey: serverKey,
		Salt:      salt,
		IterCount: iterCount,
		Roles: []storage.Role{
			{Role: "root", DB: "admin"},
		},
	}

	if err := engine.Users().CreateUser(user); err != nil {
		return fmt.Errorf("CreateAdminUser: failed to create user: %w", err)
	}

	return nil
}
