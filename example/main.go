package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/pdcgo/event_source"
	"github.com/pdcgo/schema/services/event_base/v1"
	"github.com/pdcgo/shared/custom_connect"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	closetrace, err := custom_connect.InitTracer("event-example")
	if err != nil {
		log.Fatalf("InitTracer: %v", err)
	}
	defer closetrace(context.Background())

	ctx := context.Background()
	projectID := "my-project"

	const (
		topicID  = "hello-topic"
		subID    = "push-sub"
		pushPort = "8090"
		pushPath = "/push"
	)

	pushEndpoint := fmt.Sprintf("http://localhost:%s%s", pushPort, pushPath)
	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)

	client, err := event_source.NewPubsubEmulator(ctx, projectID)
	if err != nil {
		log.Fatalf("NewPubsubEmulator: %v", err)
	}
	defer client.Close()

	// 1a. Create the topic (idempotent).
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topicName,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateTopic: %v", err)
	}

	// 1b. Create the push subscription.
	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               subName,
		Topic:              topicName,
		AckDeadlineSeconds: 20,
		PushConfig: &pubsubpb.PushConfig{
			PushEndpoint: pushEndpoint,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateSubscription: %v", err)
	}

	// 2. Register push handler on a ServeMux.
	mux := http.NewServeMux()

	mux.HandleFunc(pushPath, event_source.NewMuxPushhandler(
		func(ctx context.Context, msg event_source.PushRequest) error {
			slog.Info("receive message", "message", msg)
			return nil
		},
	))

	// 3. Publish a test message after a short delay so the server is ready.

	go periodicHello(ctx, client)

	// 4. Start the HTTP server.
	port := os.Getenv("PUSH_PORT")
	if port == "" {
		port = pushPort
	}
	log.Printf("push server listening on :%s", port)
	if err := http.ListenAndServe("localhost:"+port, mux); err != nil {
		log.Fatalf("ListenAndServe: %v", err)
	}
}

func periodicHello(ctx context.Context, client *pubsub.Client) {
	provider := otel.GetTracerProvider()
	tracer := provider.Tracer("")

	send := event_source.NewPubsubEventSender(client)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ctx, span := tracer.Start(ctx, "periodicHello")

			time.Sleep(1 * time.Second)
			event := &event_base.HelloExampleEvent{
				Name: "ekalaya",
			}

			serverId, err := send(ctx, event)
			if err != nil {
				slog.Error("publish error", slog.String("error", err.Error()))
				continue
			}

			slog.Info("publish success", slog.String("server_id", serverId))
			span.End()

		}
	}
}
