package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"

	"golang.org/x/crypto/pbkdf2"
)

// HashPassword derives SCRAM-SHA-256 stored credentials from a plaintext password.
// Returns (storedKey, serverKey, salt, iterCount, error).
// This implements the key derivation specified in RFC 5802 §3 and §5.1.
func HashPassword(password string) (storedKey, serverKey, salt []byte, iterCount int, err error) {
	iterCount = 15000
	salt = make([]byte, 16)
	if _, err = rand.Read(salt); err != nil {
		return nil, nil, nil, 0, err
	}

	// SaltedPassword := Hi(password, salt, i)
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterCount, 32, sha256.New)

	// ClientKey := HMAC(SaltedPassword, "Client Key")
	clientKeyHMAC := hmac.New(sha256.New, saltedPassword)
	clientKeyHMAC.Write([]byte("Client Key"))
	clientKeyBytes := clientKeyHMAC.Sum(nil)

	// StoredKey := H(ClientKey)
	storedKeyArr := sha256.Sum256(clientKeyBytes)
	storedKey = storedKeyArr[:]

	// ServerKey := HMAC(SaltedPassword, "Server Key")
	serverKeyHMAC := hmac.New(sha256.New, saltedPassword)
	serverKeyHMAC.Write([]byte("Server Key"))
	serverKey = serverKeyHMAC.Sum(nil)

	return storedKey, serverKey, salt, iterCount, nil
}
