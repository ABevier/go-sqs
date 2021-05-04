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

func (m *SqsMessage) ack() error {
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
	retreiveCompleteChannel := make(chan []*SqsMessage)
	workCompleteChannel := make(chan struct{})

	messageChannel := make(chan *SqsMessage, c.maxPrefetch)

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            c.queue.queueUrl,
		MaxNumberOfMessages: MAX_BATCH,
		WaitTimeSeconds:     MAX_WAIT_TIME_SECONDS,
	}

	// number of outbound requests.  min of 1 to max of config value
	// do a request
	// count number of messages pulled
	// if greater than 7 - make 2 requests
	// if less than 3 - do no make a request another request (unless 0 reuqests would be outstanding)
	// also check this against the number i'm allowed to prefetch

	go func() {
		numMessages := 0
		numInflightRetrieveRequests := 0
		retrieveRequestLimit := MINIMUM_RETRIEVE_REQUEST_LIMIT

		for !c.isShutudown {
			neededRequests := calculateNeededRetrieveRequests(numMessages, numInflightRetrieveRequests, retrieveRequestLimit)

			fmt.Printf("Consumer State: msgCnt: %v retrieveCnt: %v retrieveLimit: %v needed: %v \n",
				numMessages, numInflightRetrieveRequests, retrieveRequestLimit, neededRequests)

			for i := 0; i < neededRequests; i++ {
				go receiveMessages(c.queue, input, retreiveCompleteChannel)
			}
			numInflightRetrieveRequests += neededRequests

			select {
			case resp := <-retreiveCompleteChannel:
				count := len(resp)
				numMessages += count
				retrieveRequestLimit = calculateNewRetrieveRequestLimit(retrieveRequestLimit, count)
				numInflightRetrieveRequests--

				//TODO: is gross
				go func() {
					for _, m := range resp {
						messageChannel <- m
					}
				}()

			case <-workCompleteChannel:
				numMessages--
			}
		}

		//TODO: this is probably not great?
		close(messageChannel)
	}()

	// Start a bunch of workers
	for i := 0; i < c.maxProcessing; i++ {
		go func(id int) {
			for msg := range messageChannel {
				fmt.Printf("Handling message: %v on worker: %v\n", *msg.body, id)
				processMessage(msg, c.callbackFunc)
				workCompleteChannel <- struct{}{}
			}
		}(i)
	}
}

func receiveMessages(queue *SqsQueue, input *sqs.ReceiveMessageInput, responseChannel chan []*SqsMessage) {
	//TODO: real context? do I actually want to cancel this on shutdown?
	output, err := queue.client.ReceiveMessage(context.TODO(), input)
	if err != nil {
		//TODO: log to a provided logger?
		responseChannel <- make([]*SqsMessage, 0)
	} else {
		sqsMessages := make([]*SqsMessage, 0, 10)

		for _, message := range output.Messages {
			sqsMessage := &SqsMessage{
				body:          message.Body,
				receiptHandle: message.ReceiptHandle,
				queue:         queue,
			}
			sqsMessages = append(sqsMessages, sqsMessage)
		}
		responseChannel <- sqsMessages
	}
}

func processMessage(msg *SqsMessage, callback MessageCallbackFunc) {
	//TODO: check return of the callback type and take a different action beside just acking?
	callback(msg.body)
	err := msg.ack()
	if err != nil {
		fmt.Printf("err acking - what do? %v", err)
	}
}

func (c *SqsQueueConsumer) Shutdown() {
	c.isShutudown = true
}
