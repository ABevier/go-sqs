package internal

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SqsQueue struct {
	queueUrl       *string
	client         *sqs.Client
	sendExecutor   *BatchExecutor
	deleteExecutor *BatchExecutor
}

type sendMessageRequest struct {
	body         string
	delaySeconds int
	done         chan<- string
}

type deleteMessageRequest struct {
	receiptHandle string
	done          chan<- string
}

func NewSqsQueue(queueUrl string, maxLinger time.Duration) *SqsQueue {
	q := &SqsQueue{
		queueUrl: &queueUrl,
	}
	q.sendExecutor = NewBatchExecutor(maxLinger, q.executeSendBatch)
	q.deleteExecutor = NewBatchExecutor(maxLinger, q.executeDeleteBatch)
	return q
}

func (q *SqsQueue) SendMessage(messageBody string) string {
	waitChannel := make(chan string)
	request := &sendMessageRequest{
		body: messageBody,
		done: waitChannel,
	}

	q.sendExecutor.AddItem(request)

	id := <-waitChannel
	return id
}

func (q *SqsQueue) executeSendBatch(b *Batch) {
	//TODO: use const
	entries := make([]types.SendMessageBatchRequestEntry, 0, 10)

	for i, value := range b.Buffer {
		sendRequest := value.(*sendMessageRequest)
		id := strconv.Itoa(i)
		entry := types.SendMessageBatchRequestEntry{
			Id:           &id,
			MessageBody:  &sendRequest.body,
			DelaySeconds: int32(sendRequest.delaySeconds),
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
	}

	for _, entry := range result.Successful {
		id, err := strconv.Atoi(*entry.Id)
		if err != nil {
			//hosed....
			continue
		}

		item := b.Buffer[id].(*sendMessageRequest)
		item.done <- *entry.MessageId
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
