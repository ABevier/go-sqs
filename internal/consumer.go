package internal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const MAX_WAIT_TIME_SECONDS = 20

type SqsQueueConsumer struct {
	queue       *SqsQueue
	MessageChan <-chan string
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
	messageChannel := make(chan string)
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
				//do nothing?
			} else {
				for _, message := range output.Messages {
					messageChannel <- *message.Body
				}
			}
		}
		close(messageChannel)
	}()
}

func (c *SqsQueueConsumer) Shutdown() {
	c.isShutudown = true
}
