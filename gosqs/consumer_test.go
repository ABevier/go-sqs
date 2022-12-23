package gosqs

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestConsumer(t *testing.T) {
	client := &TestSQSClient{}

	publisher := NewPublisher(client, "queue-url", 20*time.Millisecond)

	cb := func(ctx context.Context, msg string) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	c := NewConsumer(Opts{MaxReceivedMessages: 100, MaxWorkers: 3, MaxInflightReceiveMessageRequests: 10}, publisher, cb)
	c.Start()

	time.Sleep(10 * time.Second)

	c.Shutdown()
}

type TestSQSClient struct {
}

var _ SQSClient = &TestSQSClient{}

func (c *TestSQSClient) SendMessageBatch(
	ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {

	time.Sleep(25 * time.Millisecond)

	var entries []types.SendMessageBatchResultEntry
	for _, req := range params.Entries {
		entry := types.SendMessageBatchResultEntry{Id: req.Id, MessageId: Ptr("todo")}
		entries = append(entries, entry)
	}

	return &sqs.SendMessageBatchOutput{
		Successful: entries,
	}, nil
}

func (c *TestSQSClient) DeleteMessageBatch(
	ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {

	//TODO: random sleeps
	time.Sleep(25 * time.Millisecond)

	var entries []types.DeleteMessageBatchResultEntry
	for _, req := range params.Entries {
		entry := types.DeleteMessageBatchResultEntry{Id: req.Id}
		entries = append(entries, entry)
	}

	return &sqs.DeleteMessageBatchOutput{
		Successful: entries,
	}, nil
}

func (c *TestSQSClient) ReceiveMessage(
	ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {

	time.Sleep(20 * time.Millisecond)
	resp := &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				Body:          Ptr("test message"),
				ReceiptHandle: Ptr("make this random"),
			},
		},
	}

	return resp, nil
}

func Ptr[T any](v T) *T {
	return &v
}
