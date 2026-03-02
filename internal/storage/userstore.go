package storage

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// userStore implements UserStore backed by admin.db's _users bucket.
type userStore struct {
	engine *BBoltEngine
}

// CreateUser creates a new user. Returns ErrCodeUserAlreadyExists if the user exists.
func (s *userStore) CreateUser(u User) error {
	if u.DB == "" || u.Username == "" {
		return Errorf(ErrCodeBadValue, "user must have db and username")
	}
	db, err := s.engine.openDB("admin")
	if err != nil {
		return err
	}
	data, err := json.Marshal(u)
	if err != nil {
		return fmt.Errorf("CreateUser: marshal: %w", err)
	}
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketUsers))
		if err != nil {
			return err
		}
		key := userKey(u.DB, u.Username)
		if b.Get(key) != nil {
			return Errorf(ErrCodeUserAlreadyExists, "user %q already exists in db %q", u.Username, u.DB)
		}
		return b.Put(key, data)
	})
}

// UpdateUser updates mutable fields of an existing user.
func (s *userStore) UpdateUser(db, username string, update UserUpdate) error {
	boltDB, err := s.engine.openDB("admin")
	if err != nil {
		return err
	}
	return boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketUsers))
		if b == nil {
			return Errorf(ErrCodeUserNotFound, "user %q not found in db %q", username, db)
		}
		key := userKey(db, username)
		data := b.Get(key)
		if data == nil {
			return Errorf(ErrCodeUserNotFound, "user %q not found in db %q", username, db)
		}
		var u User
		if err := json.Unmarshal(data, &u); err != nil {
			return fmt.Errorf("UpdateUser: unmarshal: %w", err)
		}
		if update.StoredKey != nil {
			u.StoredKey = update.StoredKey
		}
		if update.ServerKey != nil {
			u.ServerKey = update.ServerKey
		}
		if update.Salt != nil {
			u.Salt = update.Salt
		}
		if update.IterCount != 0 {
			u.IterCount = update.IterCount
		}
		if update.Roles != nil {
			u.Roles = update.Roles
		}
		if len(update.CustomData) > 0 {
			u.CustomData = update.CustomData
		}
		newData, err := json.Marshal(u)
		if err != nil {
			return fmt.Errorf("UpdateUser: marshal: %w", err)
		}
		return b.Put(key, newData)
	})
}

// DeleteUser removes a user. Returns ErrCodeUserNotFound if not found.
func (s *userStore) DeleteUser(db, username string) error {
	boltDB, err := s.engine.openDB("admin")
	if err != nil {
		return err
	}
	return boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketUsers))
		if b == nil {
			return Errorf(ErrCodeUserNotFound, "user %q not found in db %q", username, db)
		}
		key := userKey(db, username)
		if b.Get(key) == nil {
			return Errorf(ErrCodeUserNotFound, "user %q not found in db %q", username, db)
		}
		return b.Delete(key)
	})
}

// GetUser retrieves a user by db and username.
// Returns (User, true, nil) if found, (User{}, false, nil) if not found, and (User{}, false, err) on error.
func (s *userStore) GetUser(db, username string) (User, bool, error) {
	boltDB, err := s.engine.openDB("admin")
	if err != nil {
		return User{}, false, err
	}
	var u User
	var found bool
	if err := boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketUsers))
		if b == nil {
			return nil
		}
		key := userKey(db, username)
		data := b.Get(key)
		if data == nil {
			return nil
		}
		found = true
		return json.Unmarshal(data, &u)
	}); err != nil {
		return User{}, false, fmt.Errorf("GetUser: %w", err)
	}
	return u, found, nil
}

// ListUsers returns all users in the given database.
func (s *userStore) ListUsers(db string) ([]User, error) {
	boltDB, err := s.engine.openDB("admin")
	if err != nil {
		return nil, err
	}
	var users []User
	if err := boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketUsers))
		if b == nil {
			return nil
		}
		prefix := userPrefix(db)
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			var u User
			if err := json.Unmarshal(v, &u); err != nil {
				return fmt.Errorf("ListUsers: unmarshal: %w", err)
			}
			users = append(users, u)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return users, nil
}

// HasUser returns true if the user exists in the given database.
func (s *userStore) HasUser(db, username string) (bool, error) {
	_, found, err := s.GetUser(db, username)
	return found, err
}
