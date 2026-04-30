package event_source

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub/v2"
	"github.com/pdcgo/schema/services/event_base/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type PushMessage struct {
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	MessageID   string            `json:"messageId"`
	PublishTime string            `json:"publishTime"` // decode as string first
	OrderingKey string            `json:"orderingKey"`
}

type PushRequest struct {
	Message      PushMessage `json:"message"`
	Subscription string      `json:"subscription"`
}

func NewPubsubEmulator(ctx context.Context, projectID string) (c *pubsub.Client, err error) {
	conn, err := grpc.NewClient("localhost:8085", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn))

}

func NewPubSubDefaultClient() (c *pubsub.Client, err error) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		return nil, fmt.Errorf("GOOGLE_CLOUD_PROJECT not set")
	}
	return pubsub.NewClient(context.Background(), projectID)
}

type MessageAttributeCarrier map[string]string

func (c MessageAttributeCarrier) Get(key string) string {
	return c[key]
}

func (c MessageAttributeCarrier) Set(key string, value string) {
	c[key] = value
}

func (c MessageAttributeCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func GetTopicName(event proto.Message) string {
	option, ok := proto.GetExtension(
		event.ProtoReflect().Descriptor().Options(),
		event_base.E_EventConfig,
	).(*event_base.MessageEventConfig)

	if !ok {
		return ""
	}

	return option.EventTopic
}
