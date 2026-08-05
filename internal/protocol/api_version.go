package protocol

import "io"

// ============================================================================
// STRUCT: ApiVersionKey
// Description: Represents a supported ApiKey with its minimum and maximum versions.
// ============================================================================
type ApiVersionKey struct {
	ApiKey     int16 // API key numeric identifier supported by broker
	MinVersion int16 // Minimum API version supported for this API key
	MaxVersion int16 // Maximum API version supported for this API key
}

// ============================================================================
// STRUCT: ApiVersionResponse
// Description: Response payload for ApiKey 18 (ApiVersions).
// ============================================================================
type ApiVersionResponse struct {
	ErrorCode int16           // Error code (0 = NO_ERROR)
	ApiKeys   []ApiVersionKey // List of supported API keys and their version ranges
}

// ============================================================================
// FUNCTION: DefaultApiVersionsResponse
// Description: Returns the list of ApiKeys supported by our custom Go broker.
// ============================================================================
func DefaultApiVersionResponse() *ApiVersionResponse {
	return &ApiVersionResponse{
		ErrorCode: 0,
		ApiKeys: []ApiVersionKey{
			{ApiKey: ApiKeyProduce, MinVersion: 0, MaxVersion: 8},
			{ApiKey: ApiKeyFetch, MinVersion: 0, MaxVersion: 8},
			{ApiKey: ApiKeyMetadata, MinVersion: 0, MaxVersion: 5},
			{ApiKey: ApiKeySaslHandshake, MinVersion: 0, MaxVersion: 1},
			{ApiKey: ApiKeyApiVersions, MinVersion: 0, MaxVersion: 3},
			{ApiKey: ApiKeyInitProducerId, MinVersion: 0, MaxVersion: 3},
			{ApiKey: ApiKeyAddPartitionsToTxn, MinVersion: 0, MaxVersion: 3},
			{ApiKey: ApiKeyEndTxn, MinVersion: 0, MaxVersion: 3},
			{ApiKey: ApiKeySaslAuthenticate, MinVersion: 0, MaxVersion: 1},
		},
	}
}

// ============================================================================
// FUNCTION: EncodeApiVersionsResponse
// Description: Encodes ApiVersionsResponse into binary format to an io.Writer.
// ============================================================================
func EncodeApiVersionResponse(w io.Writer, resp *ApiVersionResponse) error {
	// Write ErrorCode (int16)
	if err := WriteInt16(w, resp.ErrorCode); err != nil {
		return err
	}

	// Write count of supported ApiKeys (int32)
	if err := WriteInt32(w, int32(len(resp.ApiKeys))); err != nil {
		return err
	}

	// Write each supported ApiKey struct
	for _, key := range resp.ApiKeys {
		if err := WriteInt16(w, key.ApiKey); err != nil {
			return err
		}
		if err := WriteInt16(w, key.MinVersion); err != nil {
			return err
		}
		if err := WriteInt16(w, key.MaxVersion); err != nil {
			return err
		}
	}

	return nil
}
