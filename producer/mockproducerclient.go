package ministreamproducer

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/nbigot/ministream-client-go/client/types"
)

type MockProducerClient struct {
	// implements interface IProducerClient
	Records []interface{}
}

func (m *MockProducerClient) Reconnect() *types.APIError {
	return nil
}

func (m *MockProducerClient) Disconnect() {
}

func (m *MockProducerClient) Authenticate(ctx context.Context) *types.APIError {
	return nil
}

func (m *MockProducerClient) PutRecords(ctx context.Context, streamUUID uuid.UUID, batchId int, records []interface{}) (*types.PutRecordsResponse, *http.Response, *types.APIError) {
	m.Records = append(m.Records, records...)
	return nil, nil, nil // not exactly the same as the original implementation
}

func NewMockProducerClient() *MockProducerClient {
	return &MockProducerClient{
		Records: make([]interface{}, 0),
	}
}
