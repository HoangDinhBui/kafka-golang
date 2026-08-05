package protocol

import (
	"io"
)

// ============================================================================
// STRUCT: SaslHandshakeRequest (ApiKey 17)
// Description: Sent by client to negotiate the SASL mechanism (PLAIN, SCRAM-SHA-256).
// ============================================================================
type SaslHandshakeRequest struct {
	Mechanism string // SASL mechanism name (e.g., "PLAIN", "SCRAM-SHA-256")
}

type SaslHandshakeResponse struct {
	ErrorCode          int16
	EnabledMechanisms []string
}

// DecodeSaslHandshakeRequest decodes SaslHandshakeRequest from stream
func DecodeSaslHandshakeRequest(r io.Reader) (*SaslHandshakeRequest, error) {
	mech, err := ReadString(r)
	if err != nil {
		return nil, err
	}
	return &SaslHandshakeRequest{Mechanism: mech}, nil
}

// EncodeSaslHandshakeResponse encodes SaslHandshakeResponse to stream
func EncodeSaslHandshakeResponse(w io.Writer, resp *SaslHandshakeResponse) error {
	if err := WriteInt16(w, resp.ErrorCode); err != nil {
		return err
	}
	if err := WriteInt32(w, int32(len(resp.EnabledMechanisms))); err != nil {
		return err
	}
	for _, mech := range resp.EnabledMechanisms {
		if err := WriteString(w, mech); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================================
// STRUCT: SaslAuthenticateRequest (ApiKey 36)
// Description: Sent by client to transmit authentication credentials payload.
// ============================================================================
type SaslAuthenticateRequest struct {
	AuthData []byte
}

type SaslAuthenticateResponse struct {
	ErrorCode    int16
	ErrorMessage *string
	AuthData     []byte
}

// DecodeSaslAuthenticateRequest decodes SaslAuthenticateRequest from stream
func DecodeSaslAuthenticateRequest(r io.Reader) (*SaslAuthenticateRequest, error) {
	data, err := ReadBytes(r)
	if err != nil {
		return nil, err
	}
	return &SaslAuthenticateRequest{AuthData: data}, nil
}

// EncodeSaslAuthenticateResponse encodes SaslAuthenticateResponse to stream
func EncodeSaslAuthenticateResponse(w io.Writer, resp *SaslAuthenticateResponse) error {
	if err := WriteInt16(w, resp.ErrorCode); err != nil {
		return err
	}
	if err := WriteNullableString(w, resp.ErrorMessage); err != nil {
		return err
	}
	return WriteBytes(w, resp.AuthData)
}
