package internal

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const MAX_BATCH = 10

type SqsQueue struct {
	queueUrl       *string
	client         *sqs.Client
	sendExecutor   *BatchExecutor
	deleteExecutor *BatchExecutor
}

type sendMessageRequest struct {
	body         string
	delaySeconds int
	done         chan<- sendMessageResult
}

type deleteMessageRequest struct {
	receiptHandle string
	done          chan<- string
}

type sendMessageResult struct {
	messageId string
	err       error
}

func NewSqsQueue(client *sqs.Client, queueUrl *string, maxLinger time.Duration) *SqsQueue {
	q := &SqsQueue{
		client:   client,
		queueUrl: queueUrl,
	}
	q.sendExecutor = NewBatchExecutor(maxLinger, q.executeSendBatch)
	q.deleteExecutor = NewBatchExecutor(maxLinger, q.executeDeleteBatch)
	return q
}

func (q *SqsQueue) SendMessage(messageBody string) (string, error) {
	waitChannel := make(chan sendMessageResult)
	request := &sendMessageRequest{
		body: messageBody,
		done: waitChannel,
	}

	q.sendExecutor.AddItem(request)

	result := <-waitChannel
	if result.err != nil {
		return "", result.err
	}

	return result.messageId, nil
}

func (q *SqsQueue) executeSendBatch(b *Batch) {
	entries := make([]types.SendMessageBatchRequestEntry, 0, MAX_BATCH)

	requests := make([]*sendMessageRequest, 0, MAX_BATCH)
	for _, value := range b.Buffer {
		request := value.(*sendMessageRequest)
		requests = append(requests, request)
	}

	for i, request := range requests {
		id := strconv.Itoa(i)
		entry := types.SendMessageBatchRequestEntry{
			Id:           &id,
			MessageBody:  &request.body,
			DelaySeconds: int32(request.delaySeconds),
		}
		entries = append(entries, entry)
	}

	batchRequest := &sqs.SendMessageBatchInput{
		QueueUrl: q.queueUrl,
		Entries:  entries,
	}

	result, err := q.client.SendMessageBatch(context.TODO(), batchRequest)
	if err != nil {
		//send errors to all waiters
		for _, request := range requests {
			request.done <- sendMessageResult{err: err}
		}
	}

	for _, entry := range result.Successful {
		idx, err := strconv.Atoi(*entry.Id)
		if err != nil {
			//hosed....
			continue
		}

		request := requests[idx]
		request.done <- sendMessageResult{messageId: *entry.MessageId}
	}

	for _, entry := range result.Failed {
		idx, err := strconv.Atoi(*entry.Id)
		if err != nil {
			//hosed....
			continue
		}

		request := requests[idx]
		//TODO: make a custom error
		request.done <- sendMessageResult{err: errors.New(*entry.Message)}
	}
}

func (q *SqsQueue) DeleteMessage(receiptHandle string) string {
	waitChannel := make(chan string)
	request := &deleteMessageRequest{
		receiptHandle: receiptHandle,
		done:          waitChannel,
	}

	q.sendExecutor.AddItem(request)

	id := <-waitChannel
	return id
}

func (q *SqsQueue) executeDeleteBatch(b *Batch) {
	// simulate a long delay to sqs
	time.Sleep(2 * time.Second)

	for _, value := range b.Buffer {
		sendRequest := value.(*deleteMessageRequest)
		sendRequest.done <- "RESULT: " + sendRequest.receiptHandle
	}
}
