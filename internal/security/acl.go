package security

import (
	"sync"
)

// ============================================================================
// CONSTANTS & TYPES: ACL ResourceTypes and Operations
// ============================================================================
const (
	ResourceTypeTopic   = "Topic"
	ResourceTypeGroup   = "Group"
	ResourceTypeCluster = "Cluster"

	OpRead     = "Read"
	OpWrite    = "Write"
	OpDescribe = "Describe"
	OpAll      = "All"

	PermAllow = "Allow"
	PermDeny  = "Deny"
)

type ACLRule struct {
	Principal      string // User principal (e.g., "User:admin" or "*")
	ResourceType   string // "Topic", "Group", "Cluster"
	ResourceName   string // Topic/Group name or "*"
	Operation      string // "Read", "Write", "Describe", "All"
	PermissionType string // "Allow", "Deny"
}

// ============================================================================
// STRUCT: ACLManager
// Description: Manages Access Control List rules and evaluates permission checks.
// ============================================================================
type ACLManager struct {
	mu    sync.RWMutex
	rules []ACLRule
}

func NewACLManager() *ACLManager {
	return &ACLManager{
		rules: make([]ACLRule, 0),
	}
}

func (am *ACLManager) AddRule(rule ACLRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules = append(am.rules, rule)
}

func (am *ACLManager) Authorize(principal string, resourceType string, resourceName string, operation string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	matched := false
	allowed := false

	for _, rule := range am.rules {
		// Match Principal ("*" or exact match)
		if rule.Principal != "*" && rule.Principal != principal && rule.Principal != "User:"+principal {
			continue
		}

		// Match ResourceType
		if rule.ResourceType != resourceType {
			continue
		}

		// Match ResourceName ("*" or exact match)
		if rule.ResourceName != "*" && rule.ResourceName != resourceName {
			continue
		}

		// Match Operation ("All" or exact match)
		if rule.Operation != OpAll && rule.Operation != operation {
			continue
		}

		matched = true

		// Deny takes absolute precedence over Allow
		if rule.PermissionType == PermDeny {
			return false
		}

		if rule.PermissionType == PermAllow {
			allowed = true
		}
	}

	// Default: if no registered rule addresses this principal/resource/
	// operation combination at all, allow access — same policy as an empty
	// rule set. Without this, a single Deny rule scoped to one resource
	// would silently flip every OTHER resource to deny-by-default too.
	if !matched {
		return true
	}

	return allowed
}
