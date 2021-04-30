package internal

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const MAX_WAIT_TIME_SECONDS = 20

type MessageCallbackFunc func(s *string)

type SqsMessage struct {
	body          *string
	receiptHandle *string
	queue         *SqsQueue
}

func (m *SqsMessage) Ack() error {
	return m.queue.DeleteMessage(m.receiptHandle)
}

type SqsQueueConsumer struct {
	queue         *SqsQueue
	maxPrefetch   int
	maxProcessing int
	callbackFunc  MessageCallbackFunc
	//TODO: atomic or better way to shut down
	isShutudown bool
}

func NewConsumer(queue *SqsQueue, maxPrefetch, maxProcessing int, callback MessageCallbackFunc) *SqsQueueConsumer {
	return &SqsQueueConsumer{
		queue:         queue,
		maxPrefetch:   maxPrefetch,
		maxProcessing: maxProcessing,
		callbackFunc:  callback,
		isShutudown:   false,
	}
}

func (c *SqsQueueConsumer) Start() {
	messageChannel := make(chan *SqsMessage, c.maxPrefetch)

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            c.queue.queueUrl,
		MaxNumberOfMessages: MAX_BATCH,
		WaitTimeSeconds:     MAX_WAIT_TIME_SECONDS,
	}

	// TODO: manage outbound requests
	// number of outbound requests.  min of 1 to max of config value
	// do a request
	// count number of messages pulled
	// if greater than 7 - make 2 requests
	// if less than 3 - do no make a request another request (unless 0 reuqests would be outstanding)
	// also check this against the number i'm allowed to prefetch

	//Fetch function
	go func() {
		defer close(messageChannel)
		for !c.isShutudown {
			//TODO: real context? do I actually want to cancel this on shutdown?
			output, err := c.queue.client.ReceiveMessage(context.TODO(), input)
			if err != nil {
				//do nothing for now
			} else {
				for _, message := range output.Messages {
					messageChannel <- &SqsMessage{
						body:          message.Body,
						receiptHandle: message.ReceiptHandle,
						queue:         c.queue,
					}
				}
			}
		}
	}()

	// Start a bunch of workers
	for i := 0; i < c.maxProcessing; i++ {
		go func(id int) {
			for msg := range messageChannel {
				fmt.Printf("Handling message: %v on worker: %v\n", *msg.body, id)
				processMessage(msg, c.callbackFunc)
			}
		}(i)
	}

}

func processMessage(msg *SqsMessage, callback MessageCallbackFunc) {
	//TODO: check return of the callback type and take a different action beside just acking
	callback(msg.body)
	err := msg.Ack()
	if err != nil {
		fmt.Printf("err acking - what do? %v", err)
	}
}

func (c *SqsQueueConsumer) Shutdown() {
	c.isShutudown = true
}
