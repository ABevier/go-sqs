package gosqs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	// MaxSQSBatch is the hard limit on sqs batch size set by AWS
	MaxSQSBatch = 10
	// MaxLongPollSeconds is the hard limit on long polling set by AWS
	MaxLongPollSeocnds = 20
)

var (
	ErrSendFailed = errors.New("failed to send to sqs")
)

type SQSPublisher struct {
	queueUrl string
	client   SQSClient
	sender   *batch.Executor[sendRequest, string]
	deleter  *batch.Executor[string, string]
}

type sendRequest struct {
	body         string
	delaySeconds int
}

func NewPublisher(client SQSClient, queueUrl string, maxLinger time.Duration) *SQSPublisher {
	q := &SQSPublisher{
		client:   client,
		queueUrl: queueUrl,
	}
	q.sender = batch.New(batch.Opts{MaxSize: MaxSQSBatch, MaxLinger: maxLinger}, q.sendBatch)
	q.deleter = batch.New(batch.Opts{MaxSize: MaxSQSBatch, MaxLinger: maxLinger}, q.deleteBatch)
	return q
}

func (p *SQSPublisher) SendMessage(ctx context.Context, body string) (string, error) {
	return p.SendMessageWithDelay(ctx, body, 0)
}

func (p *SQSPublisher) SendMessageWithDelay(ctx context.Context, body string, ds int) (string, error) {
	request := sendRequest{
		body:         body,
		delaySeconds: ds,
	}
	return p.sender.Submit(ctx, request)
}

func (p *SQSPublisher) sendBatch(msgs []sendRequest) ([]results.Result[string], error) {
	keyMap := map[string]int{}

	entries := make([]types.SendMessageBatchRequestEntry, 0, len(msgs))
	for i, msg := range msgs {
		// TODO: better to use a GUID?
		id := strconv.Itoa(i)
		entry := types.SendMessageBatchRequestEntry{
			Id:           &id,
			MessageBody:  &msg.body,
			DelaySeconds: int32(msg.delaySeconds),
		}
		entries = append(entries, entry)
		keyMap[id] = i
	}

	batchRequest := &sqs.SendMessageBatchInput{
		QueueUrl: &p.queueUrl,
		Entries:  entries,
	}

	// TODO context
	result, err := p.client.SendMessageBatch(context.TODO(), batchRequest)
	if err != nil {
		return nil, err
	}

	rs := make([]results.Result[string], len(msgs))

	for _, entry := range result.Successful {
		if entry.Id == nil || entry.MessageId == nil {
			continue
		}

		idx, ok := keyMap[*entry.Id]
		if !ok {
			continue
		}

		rs[idx] = results.Success(*entry.MessageId)
		delete(keyMap, *entry.Id)
	}

	for _, entry := range result.Failed {
		if entry.Id == nil {
			continue
		}

		idx, ok := keyMap[*entry.Id]
		if !ok {
			continue
		}

		sendErr := makeBatchError(entry)
		rs[idx] = results.Failure[string](sendErr)
		delete(keyMap, *entry.Id)
	}

	for id, idx := range keyMap {
		rs[idx] = results.Failure[string](fmt.Errorf("message %s not found in response: %w", id, ErrSendFailed))
	}

	return rs, nil
}

func (p *SQSPublisher) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := p.deleter.Submit(ctx, receiptHandle)
	return err
}

func (p *SQSPublisher) deleteBatch(receiptHandles []string) ([]results.Result[string], error) {
	keyMap := map[string]int{}

	entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(receiptHandles))
	for i, handle := range receiptHandles {
		id := strconv.Itoa(i)
		entry := types.DeleteMessageBatchRequestEntry{
			Id:            &id,
			ReceiptHandle: &handle,
		}
		entries = append(entries, entry)
	}

	batchRequest := &sqs.DeleteMessageBatchInput{
		QueueUrl: &p.queueUrl,
		Entries:  entries,
	}

	// TODO context
	result, err := p.client.DeleteMessageBatch(context.TODO(), batchRequest)
	if err != nil {
		return nil, err
	}

	rs := make([]results.Result[string], len(receiptHandles))

	for _, entry := range result.Successful {
		if entry.Id == nil {
			continue
		}

		idx, ok := keyMap[*entry.Id]
		if !ok {
			continue
		}

		rs[idx] = results.Success(*entry.Id)
		delete(keyMap, *entry.Id)
	}

	for _, entry := range result.Failed {
		if entry.Id == nil {
			continue
		}

		idx, ok := keyMap[*entry.Id]
		if !ok {
			continue
		}

		deleteErr := makeBatchError(entry)
		rs[idx] = results.Failure[string](deleteErr)
		delete(keyMap, *entry.Id)
	}

	for id, idx := range keyMap {
		rs[idx] = results.Failure[string](fmt.Errorf("message %s not found in response: %w", id, ErrSendFailed))
	}

	return rs, nil
}

func makeBatchError(entry types.BatchResultErrorEntry) error {
	code := "Unknown"
	if entry.Code == nil {
		code = *entry.Code
	}

	message := "Unknown"
	if entry.Message == nil {
		message = *entry.Message
	}

	return fmt.Errorf("code \"%s\" message \"%s\" senderfault \"%t\": %w", code, message, entry.SenderFault, ErrSendFailed)
}
