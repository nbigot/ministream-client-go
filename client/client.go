package ministreamclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	. "github.com/nbigot/ministream-client-go/client/types"

	"github.com/google/uuid"
)

type Credentials struct {
	Login    string
	Password string
}

type JWT struct {
	Token string
}

type MinistreamClientAuth struct {
	enabled bool
	creds   *Credentials
	jwt     *JWT
}

type MinistreamClient struct {
	url       string
	userAgent string
	auth      MinistreamClientAuth
	client    http.Client
	logger    *log.Logger
}

type RecordsIteratorParams struct {
	Name               *string      `json:"name,omitempty"`
	IteratorType       IteratorType `json:"iteratorType"`
	JqFilter           *string      `json:"jqFilter,omitempty"`
	MessageId          *MessageId   `json:"messageId,omitempty"`
	Timestamp          *time.Time   `json:"timestamp,omitempty"`
	MaxWaitTimeSeconds *int         `json:"maxWaitTimeSeconds,omitempty"` // long pooling (set 0 to disable)
}

func CreateClient(url string, userAgent string, creds *Credentials, insecureSkipVerifyTLS bool, timeout time.Duration, logger *log.Logger) *MinistreamClient {
	var tr *http.Transport = nil
	if insecureSkipVerifyTLS {
		tr = &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: insecureSkipVerifyTLS},
			MaxIdleConnsPerHost: 10,
		}
	}

	auth := MinistreamClientAuth{enabled: false, creds: creds, jwt: nil}
	if creds != nil && len(creds.Login) > 0 {
		auth.enabled = true
	}

	return &MinistreamClient{url: url, userAgent: userAgent, auth: auth, client: http.Client{Transport: tr, Timeout: timeout}, logger: logger}
}

func (c *MinistreamClient) Reconnect() *APIError {
	return nil
}

func (c *MinistreamClient) Disconnect() {
	c.client.CloseIdleConnections()
}

func (c *MinistreamClient) Authenticate(ctx context.Context) *APIError {
	if !c.auth.enabled || c.auth.creds == nil {
		c.auth.jwt = nil
		return nil
	}

	method := "GET"
	url := fmt.Sprintf("%s/api/v1/user/login", c.url)
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Connection"] = "keep-alive"
	headers["User-Agent"] = c.userAgent
	headers["ACCESS-KEY-ID"] = c.auth.creds.Login
	headers["SECRET-ACCESS-KEY"] = c.auth.creds.Password
	result := LoginUserResponse{}
	_, err := CallWebAPI(ctx, &c.client, method, url, nil, &headers, 200, &result, c.logger)
	if err != nil {
		if err.Code == ErrorJWTNotEnabled {
			// authentication is disabled on server side
			c.auth.enabled = false
			return nil
		} else {
			return err
		}
	}

	if result.Status != StatusSuccess {
		return &APIError{Message: ErrorUnexpected, Details: result.Status}
	}

	c.auth.jwt = &JWT{Token: result.JWT}
	return nil
}

func (c *MinistreamClient) CreateRecordsIterator(ctx context.Context, streamUUID uuid.UUID, p *RecordsIteratorParams) (*CreateRecordsIteratorResponse, *APIError) {
	bytesRequest, errMarshal := json.Marshal(p)
	if errMarshal != nil {
		return nil, &APIError{
			Message: errMarshal.Error(),
		}
	}
	bodyRequest := string(bytesRequest)
	method := "POST"
	url := fmt.Sprintf("%s/api/v1/stream/%s/iterator", c.url, streamUUID)
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Accept"] = "application/json"
	headers["Connection"] = "keep-alive"
	headers["User-Agent"] = c.userAgent
	if c.auth.enabled && c.auth.creds != nil {
		headers["Authorization"] = "Bearer " + c.auth.jwt.Token
	}
	result := CreateRecordsIteratorResponse{}

	_, err := CallWebAPI(ctx, &c.client, method, url, strings.NewReader(bodyRequest), &headers, 200, &result, c.logger)
	if err != nil {
		return nil, err
	}

	if result.Status != StatusSuccess {
		return nil, &APIError{Message: ErrorUnexpected, Details: result.Status}
	}

	return &result, nil
}

func (c *MinistreamClient) GetRecords(ctx context.Context, streamUUID uuid.UUID, streamIteratorUUID uuid.UUID, maxPullRecords int) (*GetStreamRecordsResponse, *http.Response, *APIError) {
	method := "GET"
	url := fmt.Sprintf("%s/api/v1/stream/%s/iterator/%s/records", c.url, streamUUID, streamIteratorUUID)
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Accept"] = "application/json"
	headers["Connection"] = "keep-alive"
	headers["User-Agent"] = c.userAgent
	if c.auth.enabled && c.auth.creds != nil {
		headers["Authorization"] = "Bearer " + c.auth.jwt.Token
	}
	result := GetStreamRecordsResponse{}
	resp, err := CallWebAPI(ctx, &c.client, method, url, nil, &headers, 200, &result, c.logger)
	if err != nil {
		return nil, resp, err
	}

	if result.Status != StatusSuccess {
		return nil, resp, &APIError{Message: ErrorUnexpected, Details: result.Status}
	}

	return &result, resp, nil
}

func (c *MinistreamClient) CloseRecordsIterator(ctx context.Context, streamUUID uuid.UUID, streamIteratorUUID uuid.UUID) *APIError {
	method := "DELETE"
	url := fmt.Sprintf("%s/api/v1/stream/%s/iterator/%s", c.url, streamUUID, streamIteratorUUID)
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Connection"] = "keep-alive"
	headers["User-Agent"] = c.userAgent
	if c.auth.enabled && c.auth.creds != nil {
		headers["Authorization"] = "Bearer " + c.auth.jwt.Token
	}
	result := CloseRecordsIteratorResponse{}

	// Client trace to log whether the request's underlying tcp connection was re-used.
	// clientTrace := &httptrace.ClientTrace{
	// 	GotConn: func(info httptrace.GotConnInfo) { log.Printf("conn was reused: %t", info.Reused) },
	// }
	// traceCtx := httptrace.WithClientTrace(ctx, clientTrace)
	// _, err := CallWebAPI(traceCtx, &c.client, method, url, nil, &headers, 200, &result, c.logger)

	_, err := CallWebAPI(ctx, &c.client, method, url, nil, &headers, 200, &result, c.logger)
	if err != nil {
		return err
	}

	if result.Status != StatusSuccess {
		return &APIError{Message: ErrorUnexpected, Details: result.Status}
	}

	return nil
}

func (c *MinistreamClient) PutRecords(ctx context.Context, streamUUID uuid.UUID, records []interface{}) (*PutRecordsResponse, *http.Response, *APIError) {
	method := "PUT"
	url := fmt.Sprintf("%s/api/v1/stream/%s/records", c.url, streamUUID)
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Accept"] = "application/json"
	headers["Connection"] = "keep-alive"
	headers["User-Agent"] = c.userAgent
	if c.auth.enabled && c.auth.creds != nil {
		headers["Authorization"] = "Bearer " + c.auth.jwt.Token
	}
	jsonBody, err := json.Marshal(records)
	if err != nil {
		return nil, nil, &APIError{Message: "Can't serialize records into json"}
	}
	result := PutRecordsResponse{}
	resp, apiError := CallWebAPI(ctx, &c.client, method, url, bytes.NewReader(jsonBody), &headers, 202, &result, c.logger)
	if apiError != nil {
		return nil, resp, apiError
	}

	if result.Status != StatusSuccess {
		return nil, resp, &APIError{Message: ErrorUnexpected, Details: result.Status}
	}

	return &result, resp, nil
}

func CallWebAPI[T any](
	ctx context.Context, client *http.Client, method string, url string,
	bodyRequest io.Reader, headers *map[string]string, expectedHttpStatusCode int, result T,
	logger *log.Logger,
) (*http.Response, *APIError) {
	req, err1 := http.NewRequestWithContext(ctx, method, url, bodyRequest)

	if err1 != nil {
		return nil, APIErrorFromError(err1)
	}

	if headers != nil {
		for k, v := range *headers {
			req.Header.Add(k, v)
		}
	}

	if logger != nil {
		logger.Printf("CallWebAPI: %+v\n", req)
	}

	// Don't disable the keepalive (http/1.1)
	// I'm not shure
	// https://stackoverflow.com/questions/17714494/golang-http-request-results-in-eof-errors-when-making-multiple-requests-successi
	// req.Close = true

	resp, err2 := client.Do(req)
	if err2 != nil {
		return resp, APIErrorFromError(err2)
	}

	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		// rate limiter (mitigation): the server says too many requests, try again later
		return resp, &APIError{Message: "rate limiter", Details: resp.Status, Code: ErrorTooManyRequests}
	}

	body, err3 := io.ReadAll(resp.Body)
	if err3 != nil {
		return resp, APIErrorFromError(err3)
	}

	if expectedHttpStatusCode > 0 && resp.StatusCode != expectedHttpStatusCode {
		if strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
			return resp, APIErrorFromHttpBodyResponse(body)
		} else {
			return resp, &APIError{Message: resp.Status}
		}
	}

	if err5 := json.Unmarshal(body, &result); err5 != nil {
		return resp, APIErrorFromError(err5)
	}

	return resp, nil
}

func (p *RecordsIteratorParams) Validate() *APIError {
	if p.IteratorType == IteratorTypeAtTimestamp && p.Timestamp.IsZero() {
		return &APIError{Message: "Timestamp must be set"}
	}

	// if p.IteratorType not in
	// switch p.IteratorType {
	// case IT_FIRST_MESSAGE:
	// 	return nil
	// case IT_LAST_MESSAGE:
	// 	return nil
	// case IT_AFTER_LAST_MESSAGE:
	// 	return nil
	// case IT_AT_MESSAGE_ID:
	// 	return nil
	// case IT_AFTER_MESSAGE_ID:
	// 	return nil
	// case IteratorTypeAtTimestamp:
	// 	if p.Timestamp.IsZero() {
	// 		return &APIError{Message: "Timestamp must be set"}
	// 	}
	// default:
	// 	return &APIError{Message: "invalid IteratorType"}
	// }

	return nil
}
