package event_source

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type PushHandler func(ctx context.Context, msg *PushRequest) error

func NewMuxPushhandler(handler PushHandler) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "cannot read request body", http.StatusBadRequest)
			return
		}

		msg := PushRequest{}

		err = json.Unmarshal(body, &msg)

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

		span.
			SetAttributes(
				attribute.String("event.path", r.URL.Path),
				attribute.String("event.json_data", string(body)),
			)

		err = handler(ctx, &msg)
		if err != nil {
			slog.Error("push error", slog.Any("message", msg))
			span.RecordError(err, trace.WithStackTrace(true), trace.WithAttributes(
				attribute.String("event.payload", string(body)),
			))
			span.SetAttributes(
				attribute.String("event.error", err.Error()),
			)
			span.SetStatus(codes.Error, err.Error())
			http.Error(w, "cannot handle event "+err.Error(), http.StatusInternalServerError)
			return
		}

		// ACK by returning 2xx.
		w.WriteHeader(http.StatusOK)
	}
}
