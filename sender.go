package event_source

import (
	"context"

	"buf.build/go/protovalidate"
	"cloud.google.com/go/pubsub/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type EventSender func(ctx context.Context, event proto.Message) (string, error)

func EmptySender(ctx context.Context, event proto.Message) (string, error) {
	var err error

	err = protovalidate.GlobalValidator.Validate(event)

	return "", err
}

func NewPubsubEventSender(client *pubsub.Client) EventSender {

	return func(ctx context.Context, event proto.Message) (string, error) {
		var err error

		err = protovalidate.GlobalValidator.Validate(event)
		if err != nil {
			return "", err
		}

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
