package gosqs

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"

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

func TestConsumerShutdownWithBatchExecutor(t *testing.T) {
	client := &TestSQSClient{}

	publisher := NewPublisher(client, "queue-url", 20*time.Millisecond)
	batchEx := batch.New[int, bool](
		// batch will never fill up or expire - only a flush can clear it
		batch.Opts{MaxSize: 100, MaxLinger: math.MaxInt64},
		func(reqs []int) ([]results.Result[bool], error) {
			res := make([]results.Result[bool], len(reqs))
			for idx, _ := range res {
				res[idx] = results.New(true, nil)
			}
			return res, nil
		},
	)
	cb := func(ctx context.Context, msg string) error {
		batchEx.Submit(context.Background(), 0)
		return nil
	}

	c := NewConsumer(Opts{MaxReceivedMessages: 50, MaxWorkers: 50, MaxInflightReceiveMessageRequests: 10}, publisher, cb)

	c.Start()

	time.Sleep(5 * time.Second)

	batchWg := sync.WaitGroup{}
	batchWg.Add(2)
	go func() {
		defer batchWg.Done()
		// This will start the shutdown process
		c.Shutdown()
	}()
	go func() {
		defer batchWg.Done()
		// This will wait for all in flight receive requests to complete
		c.WaitForRxShutdown()
		// This will flush the batch and unblock all workers waiting for the batch to be created
		batchEx.Flush()
	}()
	batchWg.Wait()
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
