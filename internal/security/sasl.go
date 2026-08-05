package security

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ============================================================================
// STRUCT: SASLAuthenticator
// Description: Manages registered user credentials and authenticates SASL payloads.
// ============================================================================
type SASLAuthenticator struct {
	mu           sync.RWMutex
	users        map[string]string // username -> password
	enabledMechs map[string]bool
}

func NewSASLAuthenticator() *SASLAuthenticator {
	auth := &SASLAuthenticator{
		users:        make(map[string]string),
		enabledMechs: map[string]bool{"PLAIN": true, "SCRAM-SHA-256": true},
	}
	// Default admin user
	auth.users["admin"] = "admin-secret"
	auth.users["alice"] = "alice-pass"
	return auth
}

func (sa *SASLAuthenticator) AddUser(username string, password string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	sa.users[username] = password
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

// AuthenticatePlain parses SASL/PLAIN bytes (\x00username\x00password)
func (sa *SASLAuthenticator) AuthenticatePlain(payload []byte) (string, error) {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	parts := bytes.Split(payload, []byte{0})
	if len(parts) < 3 {
		return "", errors.New("malformed SASL/PLAIN payload")
	}

	username := string(parts[1])
	password := string(parts[2])

	expectedPass, exists := sa.users[username]
	if !exists || expectedPass != password {
		return "", fmt.Errorf("authentication failed for user: %s", username)
	}

	return username, nil
}
