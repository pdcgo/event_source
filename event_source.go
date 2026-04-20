package event_source

import (
	"context"
	"encoding/json"
	"net/http"

	"cloud.google.com/go/pubsub/v2"
	"github.com/pdcgo/schema/services/event_base/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
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

type PushHandler func(ctx context.Context, msg PushRequest) error
type EventSender func(ctx context.Context, event proto.Message) (string, error)

func NewPubsubEventSender(client *pubsub.Client) EventSender {

	return func(ctx context.Context, event proto.Message) (string, error) {
		var err error

		topicName := GetTopicName(event)

		span := trace.SpanFromContext(ctx)
		eventName := string(event.ProtoReflect().Descriptor().FullName())

		span.
			SetAttributes(
				attribute.String("event.name", eventName),
				attribute.String("event.topic", topicName),
			)

		topic := client.Publisher(topicName)

		// generating raw data
		data, err := protojson.Marshal(event)
		if err != nil {
			return "", err
		}

		attibutes := MessageAttributeCarrier(map[string]string{})
		otel.GetTextMapPropagator().Inject(ctx, attibutes)

		msg := &pubsub.Message{
			Data:       data,
			Attributes: map[string]string(attibutes),
		}

		result := topic.Publish(ctx, msg)
		serverId, err := result.Get(ctx)
		if err != nil {
			return "", err
		}

		return serverId, nil
	}
}

func NewMuxPushhandler(handler PushHandler) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		msg := PushRequest{}
		err = json.NewDecoder(r.Body).Decode(&msg)

		if err != nil {
			http.Error(w, "cannot decode push request", http.StatusBadRequest)
			return
		}

		carrier := MessageAttributeCarrier(msg.Message.Attributes)
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), carrier)

		ctx, span := otel.
			Tracer("").
			Start(ctx, r.URL.Path)

		defer span.End()

		err = handler(ctx, msg)
		if err != nil {
			http.Error(w, "cannot handle event", http.StatusInternalServerError)
			return
		}

		// ACK by returning 2xx.
		w.WriteHeader(http.StatusOK)
	}
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
