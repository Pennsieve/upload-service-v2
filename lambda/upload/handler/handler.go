package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"log"
)

func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {
		log.Printf("The message %s for event source %s = %s \n\n", message.MessageId, message.EventSource, message.Body)
	}

	return nil
}
