package futures

import (
	"context"
	"encoding/json"
	"net/http"
)

// The listenKey endpoints are authenticated with the X-MBX-APIKEY header only:
// they carry NO timestamp and NO signature.

// NewListenKey creates a user-data stream listen key
// (POST /fapi/v1/listenKey, unsigned).
func (c *Client) NewListenKey(ctx context.Context) (string, error) {
	body, err := c.do(ctx, http.MethodPost, "/fapi/v1/listenKey", nil, false)
	if err != nil {
		return "", err
	}
	return decodeListenKey(body)
}

// KeepAliveListenKey extends the listen key's lifetime by 60 minutes
// (PUT /fapi/v1/listenKey, unsigned). The key is the one most recently created
// for this API key; Binance returns an empty body on success.
func (c *Client) KeepAliveListenKey(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPut, "/fapi/v1/listenKey", nil, false)
	return err
}

// CloseListenKey invalidates the listen key (DELETE /fapi/v1/listenKey,
// unsigned).
func (c *Client) CloseListenKey(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodDelete, "/fapi/v1/listenKey", nil, false)
	return err
}

func decodeListenKey(body []byte) (string, error) {
	var out struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", &transportError{err: err}
	}
	return out.ListenKey, nil
}
