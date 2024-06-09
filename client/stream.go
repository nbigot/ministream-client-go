package ministreamclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	. "github.com/nbigot/ministream-client-go/client/types"
)

func (c *MinistreamClient) CreateStream(ctx context.Context, properties *StreamProperties) (*CreateStreamResponse, error) {
	payload := struct {
		Properties *StreamProperties `json:"properties" validate:"required,lte=32,dive,keys,gt=0,lte=64,endkeys,max=128,required"`
	}{Properties: properties}

	method := "POST"
	url := fmt.Sprintf("%s/api/v1/stream/", c.url)

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("User-Agent", c.userAgent)
	if c.auth.enabled && c.auth.creds != nil {
		req.Header.Set("Authorization", "Bearer "+c.auth.jwt.Token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 201 {
		var e APIError
		if resp.Header.Get("Content-type") == "application/json" {
			if err := json.Unmarshal(body, &e); err != nil {
				return nil, fmt.Errorf(ErrorCannotUnmarshalJson)
			} else {
				return nil, &e
			}
		}
		return nil, fmt.Errorf(string(body))
	}

	var result CreateStreamResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf(ErrorCannotUnmarshalJson)
	} else {
		return &result, nil
	}
}
