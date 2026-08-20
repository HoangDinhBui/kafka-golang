package security

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// scramIterations is the PBKDF2 work factor applied to every stored
// credential (Kafka's own SCRAM default).
const scramIterations = 4096

// scramCredential holds only the salted, derived material for a user.
// The plaintext password is never retained after AddUser returns.
type scramCredential struct {
	Salt       []byte
	Iterations int
	StoredKey  []byte // H(ClientKey) — used to verify PLAIN and SCRAM logins
	ServerKey  []byte // HMAC(SaltedPassword, "Server Key") — used to prove server identity in SCRAM
}

// ============================================================================
// STRUCT: SASLAuthenticator
// Description: Manages registered user credentials and authenticates SASL payloads.
// ============================================================================
type SASLAuthenticator struct {
	mu           sync.RWMutex
	credentials  map[string]*scramCredential
	enabledMechs map[string]bool
}

func NewSASLAuthenticator() *SASLAuthenticator {
	return &SASLAuthenticator{
		credentials:  make(map[string]*scramCredential),
		enabledMechs: map[string]bool{"PLAIN": true, "SCRAM-SHA-256": true},
	}
}

// AddUser derives a salted SCRAM-SHA-256 credential from the given password
// and stores it under username. The plaintext password itself is discarded
// immediately — only the derived StoredKey/ServerKey pair is kept, so the
// same credential backs both PLAIN and SCRAM-SHA-256 logins.
func (sa *SASLAuthenticator) AddUser(username string, password string) error {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("failed to generate credential salt: %w", err)
	}

	cred := deriveCredential(password, salt, scramIterations)

	sa.mu.Lock()
	defer sa.mu.Unlock()
	sa.credentials[username] = cred
	return nil
}

func (sa *SASLAuthenticator) GetEnabledMechanisms() []string {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	var mechs []string
	for m := range sa.enabledMechs {
		mechs = append(mechs, m)
	}
	return mechs
}

func (sa *SASLAuthenticator) IsMechanismSupported(mech string) bool {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	return sa.enabledMechs[strings.ToUpper(mech)]
}

// AuthenticatePlain parses SASL/PLAIN bytes (\x00username\x00password) and
// verifies the password by re-deriving the StoredKey from the candidate
// password against the user's stored salt/iterations, then comparing in
// constant time. No plaintext password is ever stored or compared directly.
func (sa *SASLAuthenticator) AuthenticatePlain(payload []byte) (string, error) {
	parts := bytes.Split(payload, []byte{0})
	if len(parts) < 3 {
		return "", errors.New("malformed SASL/PLAIN payload")
	}

	username := string(parts[1])
	password := string(parts[2])

	sa.mu.RLock()
	cred, exists := sa.credentials[username]
	sa.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("authentication failed for user: %s", username)
	}

	candidate := deriveCredential(password, cred.Salt, cred.Iterations)
	if subtle.ConstantTimeCompare(candidate.StoredKey, cred.StoredKey) != 1 {
		return "", fmt.Errorf("authentication failed for user: %s", username)
	}

	return username, nil
}

// credential looks up the stored credential for username under a read lock.
func (sa *SASLAuthenticator) credential(username string) (*scramCredential, bool) {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	cred, exists := sa.credentials[username]
	return cred, exists
}

// deriveCredential computes the SCRAM StoredKey/ServerKey pair for a
// password using PBKDF2-HMAC-SHA256, per RFC 5802.
func deriveCredential(password string, salt []byte, iterations int) *scramCredential {
	saltedPassword := pbkdf2HMACSHA256([]byte(password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	return &scramCredential{
		Salt:       append([]byte(nil), salt...),
		Iterations: iterations,
		StoredKey:  storedKey[:],
		ServerKey:  serverKey,
	}
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}
