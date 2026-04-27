package event_source_mock

import (
	"testing"

	"github.com/pdcgo/event_source"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func NewMockEvent(t *testing.T, event proto.Message) *event_source.PushRequest {
	t.Helper()

	data, err := protojson.Marshal(event)
	if err != nil {
		t.Error(err)
		return nil
	}
	return &event_source.PushRequest{
		Message: event_source.PushMessage{
			Data: data,
		},
	}
}
