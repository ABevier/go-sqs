package internal

import (
	"time"
)

type SqsQueue struct {
	sendExecutor *BatchExecutor
}

type sendMessageRequest struct {
	body string
	done chan<- string
}

func NewSqsQueue() *SqsQueue {
	return &SqsQueue{
		sendExecutor: NewBatchExecutor(executeSendBatch),
	}
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

func executeSendBatch(b *Batch) {
	// simulate a long delay to sqs
	time.Sleep(2 * time.Second)

	for _, value := range b.Buffer {
		sendRequest := value.(*sendMessageRequest)
		sendRequest.done <- "RESULT: " + sendRequest.body
	}
}
