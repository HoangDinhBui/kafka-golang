package security

import (
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
	auth.AddUser("bob", "secret123")

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
}
