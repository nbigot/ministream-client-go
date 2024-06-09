package ministreamproducer

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nbigot/ministream-client-go/client/types"
)

func TestNewStreamProducer(t *testing.T) {
	type args struct {
		ctx             context.Context
		client          types.IProducerClient
		streamUUID      uuid.UUID
		producerHandler ProducerEventHandler
	}
	tests := []struct {
		name                   string
		args                   args
		expectedCptRecordsSent int
	}{
		{
			name: "TestNewStreamProducer",
			args: args{
				ctx:             context.Background(),
				client:          NewMockProducerClient(),
				streamUUID:      uuid.New(),
				producerHandler: NewMockProducerEventHandler(context.Background(), 30000, 10000),
			},
			expectedCptRecordsSent: 30000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := tt.args.producerHandler.(*MockProducerEventHandler)
			client := tt.args.client.(*MockProducerClient)
			producer := NewStreamProducer(tt.args.ctx, nil, 0, client, tt.args.streamUUID, handler)
			handler.Init(producer)
			producer.Run(tt.args.ctx)
			if producer.GetState() != types.ProducerStateClosed {
				t.Errorf("GetState() = %v, want %v", producer.GetState(), types.ProducerStateClosed)
			}
			// ensure that all records have been sent (the last item in the RecordQueue is nil)
			if len(client.Records) != tt.expectedCptRecordsSent {
				t.Errorf("len(client.Records) = %v, want %v", len(client.Records), tt.expectedCptRecordsSent)
			}
		})
	}
}
