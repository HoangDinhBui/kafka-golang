package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// ============================================================================
// STRUCT: SASLSession
// Description: Server-side state for one client's SASL exchange, spanning
//              the multiple SaslAuthenticate round trips SCRAM requires.
//              One session lives for the duration of a single TCP connection.
// ============================================================================
type SASLSession struct {
	Mechanism string
	Username  string

	authenticated bool

	// SCRAM-SHA-256 exchange state
	scramStep       int
	clientFirstBare string
	serverFirst     string
	fullNonce       string
	cred            *scramCredential
}

// NewSASLSession creates a fresh, unauthenticated SASL session.
func NewSASLSession() *SASLSession {
	return &SASLSession{}
}

// SetMechanism records the mechanism negotiated during SaslHandshake.
func (s *SASLSession) SetMechanism(mech string) {
	s.Mechanism = strings.ToUpper(mech)
}

// Authenticated reports whether this session has completed a successful
// SASL exchange.
func (s *SASLSession) Authenticated() bool {
	return s.authenticated
}

// Authenticate feeds one SaslAuthenticate payload into the session and
// returns the bytes to send back to the client. done reports whether the
// exchange has concluded (successfully or with an error); a false done with
// a nil error means another round trip (e.g. the SCRAM client-final-message)
// is expected next.
func (sa *SASLAuthenticator) Authenticate(session *SASLSession, payload []byte) (response []byte, done bool, username string, err error) {
	switch session.Mechanism {
	case "PLAIN":
		username, err = sa.AuthenticatePlain(payload)
		if err != nil {
			return nil, true, "", err
		}
		session.authenticated = true
		session.Username = username
		return nil, true, username, nil

	case "SCRAM-SHA-256":
		return sa.authenticateScram(session, payload)

	default:
		return nil, true, "", fmt.Errorf("no SASL mechanism negotiated (send SaslHandshake first)")
	}
}

func (sa *SASLAuthenticator) authenticateScram(session *SASLSession, payload []byte) ([]byte, bool, string, error) {
	switch session.scramStep {
	case 0:
		resp, err := sa.scramClientFirst(session, payload)
		if err != nil {
			return nil, true, "", err
		}
		return resp, false, "", nil
	case 1:
		return sa.scramClientFinal(session, payload)
	default:
		return nil, true, "", errors.New("SCRAM exchange already completed")
	}
}

// scramClientFirst handles the SCRAM client-first-message:
//
//	"n,,n=<username>,r=<client-nonce>"
//
// and replies with the server-first-message:
//
//	"r=<full-nonce>,s=<base64(salt)>,i=<iterations>"
func (sa *SASLAuthenticator) scramClientFirst(session *SASLSession, payload []byte) ([]byte, error) {
	msg := string(payload)

	bare := msg
	switch {
	case strings.HasPrefix(msg, "n,,"):
		bare = msg[len("n,,"):]
	case strings.HasPrefix(msg, "y,,"), strings.HasPrefix(msg, "p="):
		return nil, errors.New("SCRAM channel binding is not supported")
	}

	attrs, err := parseScramAttrs(bare)
	if err != nil {
		return nil, err
	}

	username := attrs["n"]
	clientNonce := attrs["r"]
	if username == "" || clientNonce == "" {
		return nil, errors.New("malformed SCRAM client-first-message")
	}

	cred, exists := sa.credential(username)
	if !exists {
		return nil, fmt.Errorf("authentication failed for user: %s", username)
	}

	serverNonceRaw := make([]byte, 18)
	if _, err := rand.Read(serverNonceRaw); err != nil {
		return nil, fmt.Errorf("failed to generate server nonce: %w", err)
	}
	fullNonce := clientNonce + base64.RawStdEncoding.EncodeToString(serverNonceRaw)

	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", fullNonce, base64.StdEncoding.EncodeToString(cred.Salt), cred.Iterations)

	session.Username = username
	session.cred = cred
	session.clientFirstBare = bare
	session.serverFirst = serverFirst
	session.fullNonce = fullNonce
	session.scramStep = 1

	return []byte(serverFirst), nil
}

// scramClientFinal handles the SCRAM client-final-message:
//
//	"c=biws,r=<full-nonce>,p=<base64(ClientProof)>"
//
// verifies ClientProof against the stored credential, and replies with the
// server-final-message "v=<base64(ServerSignature)>" on success.
func (sa *SASLAuthenticator) scramClientFinal(session *SASLSession, payload []byte) ([]byte, bool, string, error) {
	msg := string(payload)

	attrs, err := parseScramAttrs(msg)
	if err != nil {
		return nil, true, "", err
	}

	channelBinding := attrs["c"]
	nonce := attrs["r"]
	proofB64 := attrs["p"]
	if channelBinding != "biws" || nonce != session.fullNonce || proofB64 == "" {
		return nil, true, "", errors.New("malformed or invalid SCRAM client-final-message")
	}

	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil || len(clientProof) != sha256.Size {
		return nil, true, "", errors.New("invalid SCRAM client proof")
	}

	proofIdx := strings.LastIndex(msg, ",p=")
	if proofIdx < 0 {
		return nil, true, "", errors.New("malformed SCRAM client-final-message")
	}
	withoutProof := msg[:proofIdx]

	authMessage := session.clientFirstBare + "," + session.serverFirst + "," + withoutProof

	clientSignature := hmacSHA256(session.cred.StoredKey, []byte(authMessage))
	clientKey := make([]byte, len(clientSignature))
	for i := range clientKey {
		clientKey[i] = clientSignature[i] ^ clientProof[i]
	}
	candidateStoredKey := sha256.Sum256(clientKey)

	if subtle.ConstantTimeCompare(candidateStoredKey[:], session.cred.StoredKey) != 1 {
		return nil, true, "", fmt.Errorf("authentication failed for user: %s", session.Username)
	}

	serverSignature := hmacSHA256(session.cred.ServerKey, []byte(authMessage))
	serverFinal := fmt.Sprintf("v=%s", base64.StdEncoding.EncodeToString(serverSignature))

	session.authenticated = true
	session.scramStep = 2

	return []byte(serverFinal), true, session.Username, nil
}

// parseScramAttrs splits a comma-separated "key=value" attribute list as
// used by SCRAM messages. This is a deliberately minimal parser: it does not
// implement RFC 5802 saslname escaping (=2C / =3D) for usernames containing
// literal commas or equals signs, which is sufficient for the identifiers
// this broker accepts.
func parseScramAttrs(s string) (map[string]string, error) {
	attrs := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		if part == "" {
			continue
		}
		idx := strings.IndexByte(part, '=')
		if idx < 1 {
			return nil, fmt.Errorf("malformed SCRAM attribute: %q", part)
		}
		attrs[part[:idx]] = part[idx+1:]
	}
	return attrs, nil
}

// pbkdf2HMACSHA256 implements PBKDF2 (RFC 2898) with HMAC-SHA256 as the
// pseudorandom function, using only the standard library so the project's
// zero-external-dependency policy holds for the SCRAM credential KDF.
func pbkdf2HMACSHA256(password, salt []byte, iterations, keyLen int) []byte {
	const hashLen = sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen
	dk := make([]byte, 0, numBlocks*hashLen)

	mac := hmac.New(sha256.New, password)
	blockSalt := make([]byte, len(salt)+4)
	copy(blockSalt, salt)

	for block := 1; block <= numBlocks; block++ {
		binary.BigEndian.PutUint32(blockSalt[len(salt):], uint32(block))

		mac.Reset()
		mac.Write(blockSalt)
		u := mac.Sum(nil)

		t := make([]byte, len(u))
		copy(t, u)

		for i := 1; i < iterations; i++ {
			mac.Reset()
			mac.Write(u)
			u = mac.Sum(nil)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		dk = append(dk, t...)
	}
	return dk[:keyLen]
}
