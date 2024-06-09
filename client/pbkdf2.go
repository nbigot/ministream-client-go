package ministreamclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	. "github.com/nbigot/ministream-client-go/client/types"
)

type Pbkdf2Request struct {
	Digest     string `json:"digest" example:"sha512"`
	Iterations int    `json:"iterations" example:"1000"`
	Salt       string `json:"salt" example:"random0123456789azertyuiop"`
	Password   string `json:"password" example:"my secret passwprd"`
}

type Pbkdf2Response struct {
	Digest     string `json:"digest" example:"sha512"`
	Iterations int    `json:"iterations" example:"1000"`
	Salt       string `json:"salt" example:"random0123456789azertyuiop"`
	Hash       string `json:"hash" example:"$pbkdf2-sha512$i=1000$random0123456789azertyuiop$9a23999111..."`
	Status     string `json:"status" example:"success"`
}

func (c *MinistreamClient) Pbkdf2(ctx context.Context, r *Pbkdf2Request) (*Pbkdf2Response, error) {
	jsonData, _ := json.Marshal(*r)
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/api/v1/utils/pbkdf2", c.url), bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Connection", "keep-alive")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result Pbkdf2Response
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf(ErrorCannotUnmarshalJson)
	}

	return &result, nil
}

func MakePbkdf2Request(password string) *Pbkdf2Request {
	return &Pbkdf2Request{
		Digest:     "sha512",
		Iterations: 1000,
		Salt:       randomString(64),
		Password:   password,
	}
}

func randomString(length int) string {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b)[:length]
}
