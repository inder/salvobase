package auth

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/xdg-go/scram"
	"golang.org/x/crypto/pbkdf2"

	"github.com/inder/salvobase/internal/storage"
)

// TestHashPassword verifies SCRAM-SHA-256 key derivation produces valid credentials.
func TestHashPassword(t *testing.T) {
	storedKey, serverKey, salt, iterCount, err := HashPassword("testpassword")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if iterCount != 15000 {
		t.Errorf("expected iterCount=15000, got %d", iterCount)
	}
	if len(salt) != 16 {
		t.Errorf("expected 16-byte salt, got %d", len(salt))
	}
	if len(storedKey) != 32 {
		t.Errorf("expected 32-byte storedKey (SHA-256), got %d", len(storedKey))
	}
	if len(serverKey) != 32 {
		t.Errorf("expected 32-byte serverKey (SHA-256), got %d", len(serverKey))
	}

	// Verify the derived keys match manual computation.
	saltedPassword := pbkdf2.Key([]byte("testpassword"), salt, iterCount, 32, sha256.New)
	ckMAC := hmac.New(sha256.New, saltedPassword)
	ckMAC.Write([]byte("Client Key"))
	clientKey := ckMAC.Sum(nil)
	expectedStored := sha256.Sum256(clientKey)
	if !hmac.Equal(storedKey, expectedStored[:]) {
		t.Error("storedKey does not match manual derivation")
	}
	skMAC := hmac.New(sha256.New, saltedPassword)
	skMAC.Write([]byte("Server Key"))
	expectedServer := skMAC.Sum(nil)
	if !hmac.Equal(serverKey, expectedServer) {
		t.Error("serverKey does not match manual derivation")
	}
}

// TestHashPasswordSHA1 verifies SCRAM-SHA-1 key derivation produces valid credentials.
func TestHashPasswordSHA1(t *testing.T) {
	storedKey, serverKey, salt, iterCount, err := HashPasswordSHA1("testpassword")
	if err != nil {
		t.Fatalf("HashPasswordSHA1: %v", err)
	}
	if iterCount != 10000 {
		t.Errorf("expected iterCount=10000, got %d", iterCount)
	}
	if len(salt) != 16 {
		t.Errorf("expected 16-byte salt, got %d", len(salt))
	}
	if len(storedKey) != 20 {
		t.Errorf("expected 20-byte storedKey (SHA-1), got %d", len(storedKey))
	}
	if len(serverKey) != 20 {
		t.Errorf("expected 20-byte serverKey (SHA-1), got %d", len(serverKey))
	}

	// Verify the derived keys match manual computation.
	saltedPassword := pbkdf2.Key([]byte("testpassword"), salt, iterCount, 20, sha1.New) //nolint:gosec
	ckMAC := hmac.New(sha1.New, saltedPassword)                                         //nolint:gosec
	ckMAC.Write([]byte("Client Key"))
	clientKey := ckMAC.Sum(nil)
	expectedStored := sha1.Sum(clientKey) //nolint:gosec
	if !hmac.Equal(storedKey, expectedStored[:]) {
		t.Error("storedKey does not match manual derivation")
	}
	skMAC := hmac.New(sha1.New, saltedPassword) //nolint:gosec
	skMAC.Write([]byte("Server Key"))
	expectedServer := skMAC.Sum(nil)
	if !hmac.Equal(serverKey, expectedServer) {
		t.Error("serverKey does not match manual derivation")
	}
}

// TestHashPasswordDifferentSalts ensures two calls produce different salts.
func TestHashPasswordDifferentSalts(t *testing.T) {
	_, _, salt1, _, _ := HashPassword("same")
	_, _, salt2, _, _ := HashPassword("same")
	if hmac.Equal(salt1, salt2) {
		t.Error("two HashPassword calls should produce different salts")
	}

	_, _, salt3, _, _ := HashPasswordSHA1("same")
	_, _, salt4, _, _ := HashPasswordSHA1("same")
	if hmac.Equal(salt3, salt4) {
		t.Error("two HashPasswordSHA1 calls should produce different salts")
	}
}

// mockUserStore is a minimal UserStore for testing SCRAM conversations.
type mockUserStore struct {
	users map[string]storage.User
}

func (m *mockUserStore) CreateUser(u storage.User) error                     { return nil }
func (m *mockUserStore) UpdateUser(string, string, storage.UserUpdate) error { return nil }
func (m *mockUserStore) DeleteUser(string, string) error                     { return nil }
func (m *mockUserStore) ListUsers(string) ([]storage.User, error)            { return nil, nil }
func (m *mockUserStore) HasUser(db, username string) (bool, error) {
	_, ok := m.users[db+"."+username]
	return ok, nil
}
func (m *mockUserStore) GetUser(db, username string) (storage.User, bool, error) {
	u, ok := m.users[db+"."+username]
	return u, ok, nil
}

// TestSASLStartSHA256 verifies a full SCRAM-SHA-256 conversation via SASLStart/Continue.
func TestSASLStartSHA256(t *testing.T) {
	testSASLConversation(t, "SCRAM-SHA-256")
}

// TestSASLStartSHA1 verifies a full SCRAM-SHA-1 conversation via SASLStart/Continue.
func TestSASLStartSHA1(t *testing.T) {
	testSASLConversation(t, "SCRAM-SHA-1")
}

func testSASLConversation(t *testing.T, mechanism string) {
	t.Helper()
	password := "s3cret"

	// Generate credentials for both mechanisms.
	storedKey256, serverKey256, salt256, iter256, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	storedKey1, serverKey1, salt1, iter1, err := HashPasswordSHA1(password)
	if err != nil {
		t.Fatal(err)
	}

	store := &mockUserStore{users: map[string]storage.User{
		"testdb.alice": {
			Username:      "alice",
			DB:            "testdb",
			StoredKey:     storedKey256,
			ServerKey:     serverKey256,
			Salt:          salt256,
			IterCount:     iter256,
			StoredKeySHA1: storedKey1,
			ServerKeySHA1: serverKey1,
			SaltSHA1:      salt1,
			IterCountSHA1: iter1,
		},
	}}

	mgr := NewManager(store, false)

	// Build a SCRAM client to drive the conversation.
	var hashGen scram.HashGeneratorFcn
	if mechanism == "SCRAM-SHA-1" {
		hashGen = scram.SHA1
	} else {
		hashGen = scram.SHA256
	}

	client, err := hashGen.NewClient("alice", password, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	conv := client.NewConversation()

	// Step 1: client-first-message
	clientFirst, err := conv.Step("")
	if err != nil {
		t.Fatalf("client step 1: %v", err)
	}

	serverFirst, convID, err := mgr.SASLStart("testdb", mechanism, []byte(clientFirst))
	if err != nil {
		t.Fatalf("SASLStart: %v", err)
	}

	// Step 2: client-final-message
	clientFinal, err := conv.Step(string(serverFirst))
	if err != nil {
		t.Fatalf("client step 2: %v", err)
	}

	serverFinal, done, err := mgr.SASLContinue(convID, []byte(clientFinal))
	if err != nil {
		t.Fatalf("SASLContinue step 2: %v", err)
	}

	// Step 3: verify server signature
	_, err = conv.Step(string(serverFinal))
	if err != nil {
		t.Fatalf("client step 3 (verify server sig): %v", err)
	}

	if !done {
		// The xdg-go/scram library marks done after the server-final step.
		// Some implementations need one more empty Continue to finalize.
		_, done, err = mgr.SASLContinue(convID, []byte(""))
		if err != nil {
			t.Fatalf("SASLContinue final: %v", err)
		}
		if !done {
			t.Fatal("expected conversation to be done after final step")
		}
	}

	username, db, ok := mgr.GetAuthenticatedUser(convID)
	if !ok {
		t.Fatal("expected authenticated user")
	}
	if username != "alice" || db != "testdb" {
		t.Errorf("got user=%q db=%q, want alice/testdb", username, db)
	}
}

// TestSASLStartWrongPassword verifies that a wrong password is rejected.
func TestSASLStartWrongPassword(t *testing.T) {
	for _, mech := range []string{"SCRAM-SHA-256", "SCRAM-SHA-1"} {
		t.Run(mech, func(t *testing.T) {
			storedKey256, serverKey256, salt256, iter256, _ := HashPassword("correct")
			storedKey1, serverKey1, salt1, iter1, _ := HashPasswordSHA1("correct")

			store := &mockUserStore{users: map[string]storage.User{
				"testdb.bob": {
					Username:      "bob",
					DB:            "testdb",
					StoredKey:     storedKey256,
					ServerKey:     serverKey256,
					Salt:          salt256,
					IterCount:     iter256,
					StoredKeySHA1: storedKey1,
					ServerKeySHA1: serverKey1,
					SaltSHA1:      salt1,
					IterCountSHA1: iter1,
				},
			}}

			mgr := NewManager(store, false)

			var hashGen scram.HashGeneratorFcn
			if mech == "SCRAM-SHA-1" {
				hashGen = scram.SHA1
			} else {
				hashGen = scram.SHA256
			}

			client, _ := hashGen.NewClient("bob", "wrong-password", "")
			conv := client.NewConversation()
			clientFirst, _ := conv.Step("")

			serverFirst, convID, err := mgr.SASLStart("testdb", mech, []byte(clientFirst))
			if err != nil {
				t.Fatalf("SASLStart: %v", err)
			}

			clientFinal, _ := conv.Step(string(serverFirst))
			_, _, err = mgr.SASLContinue(convID, []byte(clientFinal))
			if err == nil {
				t.Error("expected authentication to fail with wrong password")
			}
		})
	}
}

// TestSASLStartUnsupportedMechanism verifies that unsupported mechanisms are rejected.
func TestSASLStartUnsupportedMechanism(t *testing.T) {
	mgr := NewManager(&mockUserStore{}, false)
	_, _, err := mgr.SASLStart("db", "PLAIN", []byte("n,,n=user,r=nonce"))
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("expected unsupported mechanism error, got: %v", err)
	}
}

// TestSASLStartUserNotFound verifies that a non-existent user is rejected.
func TestSASLStartUserNotFound(t *testing.T) {
	store := &mockUserStore{users: map[string]storage.User{}}
	mgr := NewManager(store, false)

	for _, mech := range []string{"SCRAM-SHA-256", "SCRAM-SHA-1"} {
		t.Run(mech, func(t *testing.T) {
			_, _, err := mgr.SASLStart("testdb", mech, []byte("n,,n=nobody,r=randomnonce123"))
			if err == nil || !strings.Contains(err.Error(), "failed") {
				t.Errorf("expected auth failure for non-existent user, got: %v", err)
			}
		})
	}
}

// TestSASLStartSHA1NoCredentials verifies that SHA-1 auth fails when user has no SHA-1 credentials.
func TestSASLStartSHA1NoCredentials(t *testing.T) {
	storedKey256, serverKey256, salt256, iter256, _ := HashPassword("pass")

	store := &mockUserStore{users: map[string]storage.User{
		"testdb.sha256only": {
			Username:  "sha256only",
			DB:        "testdb",
			StoredKey: storedKey256,
			ServerKey: serverKey256,
			Salt:      salt256,
			IterCount: iter256,
			// No SHA-1 credentials
		},
	}}

	mgr := NewManager(store, false)
	_, _, err := mgr.SASLStart("testdb", "SCRAM-SHA-1", []byte("n,,n=sha256only,r=randomnonce123"))
	if err == nil {
		t.Error("expected auth failure when user has no SHA-1 credentials")
	}
}

// TestMechanismNegotiation verifies the same user can auth with both mechanisms.
func TestMechanismNegotiation(t *testing.T) {
	for _, mech := range []string{"SCRAM-SHA-256", "SCRAM-SHA-1"} {
		t.Run(mech, func(t *testing.T) {
			testSASLConversation(t, mech)
		})
	}
}

// TestParseUsernameFromClientFirst covers the username parser.
func TestParseUsernameFromClientFirst(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"n,,n=alice,r=rOprNGfwEbeRWgbNEkqO", "alice", false},
		{"n,,n=bob=3Dspecial,r=nonce", "bob=special", false},
		{"n,,n=user=2Cwith=2Ccommas,r=nonce", "user,with,commas", false},
		{"n,a=authzid,n=carol,r=nonce", "carol", false},
		{"bad", "", true},
		{"n,", "", true},
		{"n,,r=nonceonly", "", true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			got, err := parseUsernameFromClientFirst(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
