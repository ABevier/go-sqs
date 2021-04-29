package internal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const MAX_WAIT_TIME_SECONDS = 20

type SqsMessage struct {
	Body          *string
	receiptHandle *string
	queue         *SqsQueue
}

func (m *SqsMessage) Ack() error {
	return m.queue.DeleteMessage(m.receiptHandle)
}

type SqsQueueConsumer struct {
	queue       *SqsQueue
	MessageChan <-chan *SqsMessage
	//TODO: atomic or better way to shut down
	isShutudown bool
}

func NewConsumer(queue *SqsQueue) *SqsQueueConsumer {
	return &SqsQueueConsumer{
		queue:       queue,
		isShutudown: false,
	}
}

func (c *SqsQueueConsumer) Start() {
	messageChannel := make(chan *SqsMessage)
	c.MessageChan = messageChannel

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            c.queue.queueUrl,
		MaxNumberOfMessages: MAX_BATCH,
		WaitTimeSeconds:     MAX_WAIT_TIME_SECONDS,
	}

	go func() {
		for !c.isShutudown {
			//TODO: real context? do I actually want to cancel this on shutdown?
			output, err := c.queue.client.ReceiveMessage(context.TODO(), input)
			if err != nil {
				//do nothing for now
			} else {
				for _, message := range output.Messages {
					messageChannel <- &SqsMessage{
						Body:          message.Body,
						receiptHandle: message.ReceiptHandle,
						queue:         c.queue,
					}
				}
			}
		}
		close(messageChannel)
	}()
}

func (c *SqsQueueConsumer) Shutdown() {
	c.isShutudown = true
}
