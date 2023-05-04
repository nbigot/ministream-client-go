package ministreamclient

import (
	"context"
	"fmt"
	"net/http"
)

func (c *MinistreamClient) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/v1/utils/ping", c.url), nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Connection", "keep-alive")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Authenticate failed")
	}
	return nil
}
