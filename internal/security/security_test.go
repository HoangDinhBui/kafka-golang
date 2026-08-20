package security

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

func TestTLSConfigGeneration(t *testing.T) {
	config, err := GenerateSelfSignedTLSConfig()
	if err != nil {
		t.Fatalf("GenerateSelfSignedTLSConfig failed: %v", err)
	}

	if len(config.Certificates) == 0 {
		t.Error("Expected at least 1 certificate in TLS config")
	}
}

func TestSASLAuthenticator_Plain(t *testing.T) {
	auth := NewSASLAuthenticator()
	if err := auth.AddUser("bob", "secret123"); err != nil {
		t.Fatalf("AddUser failed: %v", err)
	}

	// Payload format: \x00bob\x00secret123
	payload := []byte("\x00bob\x00secret123")
	username, err := auth.AuthenticatePlain(payload)
	if err != nil {
		t.Fatalf("AuthenticatePlain failed: %v", err)
	}

	if username != "bob" {
		t.Errorf("Expected username 'bob', got '%s'", username)
	}

	// Invalid password
	badPayload := []byte("\x00bob\x00wrongpass")
	_, err = auth.AuthenticatePlain(badPayload)
	if err == nil {
		t.Error("Expected error for invalid password, got nil")
	}
}

func TestSASLAuthenticator_NoHardcodedDefaultUsers(t *testing.T) {
	auth := NewSASLAuthenticator()

	// Regression guard: previously "admin"/"alice" were seeded with
	// hardcoded plaintext passwords in NewSASLAuthenticator. No account
	// should exist until AddUser is called explicitly.
	if _, exists := auth.credential("admin"); exists {
		t.Error("Expected no default 'admin' credential to be seeded")
	}
	if _, err := auth.AuthenticatePlain([]byte("\x00admin\x00admin-secret")); err == nil {
		t.Error("Expected the old hardcoded admin/admin-secret credential to no longer authenticate")
	}
}

// TestSASLAuthenticator_ScramSHA256 drives a full RFC 5802 SCRAM-SHA-256
// exchange, acting as the client, against the server-side session to
// verify the handshake computes matching proofs end-to-end.
func TestSASLAuthenticator_ScramSHA256(t *testing.T) {
	auth := NewSASLAuthenticator()
	if err := auth.AddUser("carol", "s3cr3t-pass"); err != nil {
		t.Fatalf("AddUser failed: %v", err)
	}

	session := NewSASLSession()
	session.SetMechanism("SCRAM-SHA-256")

	clientNonce := "fyko+d2lbbFgONRv9qkxdawL"
	clientFirstBare := "n=carol,r=" + clientNonce
	clientFirst := "n,," + clientFirstBare

	serverFirstResp, done, _, err := auth.Authenticate(session, []byte(clientFirst))
	if err != nil {
		t.Fatalf("client-first-message failed: %v", err)
	}
	if done {
		t.Fatal("expected exchange to continue after client-first-message")
	}

	serverFirst := string(serverFirstResp)
	attrs, err := parseScramAttrs(serverFirst)
	if err != nil {
		t.Fatalf("failed to parse server-first-message: %v", err)
	}
	fullNonce := attrs["r"]
	salt, err := base64.StdEncoding.DecodeString(attrs["s"])
	if err != nil {
		t.Fatalf("failed to decode salt: %v", err)
	}
	if !strings.HasPrefix(fullNonce, clientNonce) {
		t.Fatalf("expected server nonce to extend client nonce, got %q", fullNonce)
	}

	var iterations int
	if _, err := fmt.Sscanf(attrs["i"], "%d", &iterations); err != nil {
		t.Fatalf("failed to parse iteration count: %v", err)
	}

	// Recompute the client-side SCRAM values exactly as a real client would.
	saltedPassword := pbkdf2HMACSHA256([]byte("s3cr3t-pass"), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	clientFinalWithoutProof := "c=biws,r=" + fullNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))
	clientProof := make([]byte, len(clientKey))
	for i := range clientProof {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}
	clientFinal := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof)

	serverFinalResp, done, username, err := auth.Authenticate(session, []byte(clientFinal))
	if err != nil {
		t.Fatalf("client-final-message failed: %v", err)
	}
	if !done {
		t.Fatal("expected exchange to conclude after client-final-message")
	}
	if username != "carol" {
		t.Errorf("expected username 'carol', got %q", username)
	}
	if !session.Authenticated() {
		t.Error("expected session to be marked authenticated")
	}

	expectedServerSignature := hmacSHA256(serverKey, []byte(authMessage))
	wantServerFinal := "v=" + base64.StdEncoding.EncodeToString(expectedServerSignature)
	if string(serverFinalResp) != wantServerFinal {
		t.Errorf("server-final-message mismatch: got %q, want %q", serverFinalResp, wantServerFinal)
	}
}

// TestSASLAuthenticator_ScramSHA256_WrongPassword verifies that a client
// proof computed from the wrong password is rejected.
func TestSASLAuthenticator_ScramSHA256_WrongPassword(t *testing.T) {
	auth := NewSASLAuthenticator()
	if err := auth.AddUser("dave", "correct-password"); err != nil {
		t.Fatalf("AddUser failed: %v", err)
	}

	session := NewSASLSession()
	session.SetMechanism("SCRAM-SHA-256")

	clientNonce := "clientnonce123"
	clientFirstBare := "n=dave,r=" + clientNonce
	serverFirstResp, _, _, err := auth.Authenticate(session, []byte("n,,"+clientFirstBare))
	if err != nil {
		t.Fatalf("client-first-message failed: %v", err)
	}

	serverFirst := string(serverFirstResp)
	attrs, _ := parseScramAttrs(serverFirst)
	fullNonce := attrs["r"]

	// Forge a proof using the wrong password's derived key.
	wrongSalted := pbkdf2HMACSHA256([]byte("totally-wrong"), []byte("bogus-salt"), scramIterations, sha256.Size)
	wrongClientKey := hmacSHA256(wrongSalted, []byte("Client Key"))
	wrongStoredKey := sha256.Sum256(wrongClientKey)

	clientFinalWithoutProof := "c=biws,r=" + fullNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	forgedSignature := hmacSHA256(wrongStoredKey[:], []byte(authMessage))
	forgedProof := make([]byte, len(wrongClientKey))
	for i := range forgedProof {
		forgedProof[i] = wrongClientKey[i] ^ forgedSignature[i]
	}
	clientFinal := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(forgedProof)

	_, done, _, err := auth.Authenticate(session, []byte(clientFinal))
	if err == nil {
		t.Error("expected authentication failure for forged SCRAM proof")
	}
	if !done {
		t.Error("expected exchange to conclude (with failure) after client-final-message")
	}
}

func TestACLManager_Authorization(t *testing.T) {
	acl := NewACLManager()

	// Default empty rules -> Allow all
	if !acl.Authorize("alice", ResourceTypeTopic, "orders", OpRead) {
		t.Error("Expected default empty ACL to allow access")
	}

	// Add Allow rule for admin
	acl.AddRule(ACLRule{
		Principal:      "admin",
		ResourceType:   ResourceTypeTopic,
		ResourceName:   "*",
		Operation:      OpAll,
		PermissionType: PermAllow,
	})

	// Add Deny rule for guest on topic "secrets"
	acl.AddRule(ACLRule{
		Principal:      "guest",
		ResourceType:   ResourceTypeTopic,
		ResourceName:   "secrets",
		Operation:      OpRead,
		PermissionType: PermDeny,
	})

	if !acl.Authorize("admin", ResourceTypeTopic, "orders", OpWrite) {
		t.Error("Expected admin to be authorized")
	}

	if acl.Authorize("guest", ResourceTypeTopic, "secrets", OpRead) {
		t.Error("Expected guest to be denied on secrets topic")
	}

	// Regression: a Deny rule scoped to one resource must not flip every
	// OTHER resource to deny-by-default for principals it doesn't mention.
	if !acl.Authorize("guest", ResourceTypeTopic, "public-topic", OpRead) {
		t.Error("Expected guest to remain authorized on an unrelated topic not covered by any rule")
	}
}
