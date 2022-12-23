package gosqs

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type MessageCallbackFunc func(ctx context.Context, s string) error

type SQSMessage struct {
	body          string
	receiptHandle string
	publisher     *SQSPublisher
}

func (m *SQSMessage) ack() error {
	return m.publisher.DeleteMessage(context.TODO(), m.receiptHandle)
}

type SQSConsumer struct {
	publisher *SQSPublisher

	maxReceivedMessages               int
	maxWorkers                        int
	maxInflightReceiveMessageRequests int

	callbackFunc MessageCallbackFunc

	workerWG     *sync.WaitGroup
	isShutudown  uint32
	shutdownChan chan struct{}
}

func NewConsumer(opts Opts, publisher *SQSPublisher, callback MessageCallbackFunc) *SQSConsumer {
	return &SQSConsumer{
		publisher: publisher,

		maxReceivedMessages:               opts.MaxReceivedMessages,
		maxWorkers:                        opts.MaxWorkers,
		maxInflightReceiveMessageRequests: opts.MaxInflightReceiveMessageRequests,

		callbackFunc: callback,

		workerWG:     &sync.WaitGroup{},
		isShutudown:  0,
		shutdownChan: make(chan struct{}),
	}
}

func (c *SQSConsumer) Start() {
	msgProcessingCompleteChannel := make(chan struct{})

	messageChan := make(chan SQSMessage, c.maxReceivedMessages)

	// number of outbound requests.  min of 1 to max of config value
	// do a request
	// count number of messages pulled
	// if greater than 7 - make 2 requests
	// if less than 3 - do no make a request another request (unless 0 reuqests would be outstanding)
	// also check this against the number i'm allowed to prefetch

	go func() {
		retreivedMsgChan := make(chan []SQSMessage)

		numReceivedMessages := 0
		numInflightRetrieveRequests := 0
		retrieveRequestLimit := minOutstandingReceiveRequests

		calculator := newCalculator(c.maxReceivedMessages, c.maxInflightReceiveMessageRequests)

		for {
			if atomic.LoadUint32(&c.isShutudown) == 1 {
				if numInflightRetrieveRequests == 0 && numReceivedMessages == 0 {
					break
				}
			} else {
				neededRequests := calculator.NeededReceiveRequests(numReceivedMessages, numInflightRetrieveRequests, retrieveRequestLimit)

				// fmt.Printf("Consumer State: msgCnt: %v retrieveCnt: %v retrieveLimit: %v needed: %v \n",
				// 	numMessages, numInflightRetrieveRequests, retrieveRequestLimit, neededRequests)

				for i := 0; i < neededRequests; i++ {
					go func() {
						receiveMessageWorker(c.publisher, retreivedMsgChan)
					}()
				}
				numInflightRetrieveRequests += neededRequests
			}

			select {
			case msgs := <-retreivedMsgChan:
				numInflightRetrieveRequests--
				numReceivedMessages += len(msgs)
				retrieveRequestLimit = calculator.NewReceiveRequestLimit(retrieveRequestLimit, len(msgs))
				for _, m := range msgs {
					messageChan <- m
				}

			case <-msgProcessingCompleteChannel:
				numReceivedMessages--
			}
		}

		// All writers to these channels should have completed by the time the above loop exits
		close(retreivedMsgChan)
		close(messageChan)
	}()

	// Start a bunch of workers
	for i := 0; i < c.maxWorkers; i++ {
		c.workerWG.Add(1)
		go func(id int) {
			defer c.workerWG.Done()

			for msg := range messageChan {
				log.Printf("Handling message: %v on worker: %v\n", msg.body, id)
				c.processMessage(msg)
				msgProcessingCompleteChannel <- struct{}{}
			}
		}(i)
	}
}

func receiveMessageWorker(publisher *SQSPublisher, receivedMsgChan chan<- []SQSMessage) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &publisher.queueUrl,
		MaxNumberOfMessages: MaxSQSBatch,
		WaitTimeSeconds:     MaxLongPollSeocnds,
	}

	//TODO: real context?
	output, err := publisher.client.ReceiveMessage(context.TODO(), input)
	if err != nil {
		//TODO: log to a provided logger?
		receivedMsgChan <- nil
	} else {
		msgs := make([]SQSMessage, 0, len(output.Messages))

		for _, msg := range output.Messages {
			if msg.Body == nil || msg.ReceiptHandle == nil {
				// TODO: LOG something this is bad!!
				continue
			}
			m := SQSMessage{
				body:          *msg.Body,
				receiptHandle: *msg.ReceiptHandle,
				publisher:     publisher,
			}
			msgs = append(msgs, m)
		}
		receivedMsgChan <- msgs
	}
}

func (c *SQSConsumer) processMessage(msg SQSMessage) {
	ctx := context.TODO()
	//TODO: check return of the callback type and take a different action beside just acking?
	if err := c.callbackFunc(ctx, msg.body); err != nil {
		// TODO: look up a good way to do library level logging in go
		// TODO: log something??
		return
	}

	if err := msg.ack(); err != nil {
		// TODO: log something??
		fmt.Printf("err acking - what do? %v", err)
	}
}

func (c *SQSConsumer) Shutdown() {
	if atomic.CompareAndSwapUint32(&c.isShutudown, 0, 1) {
		go func() {
			c.workerWG.Wait()
			close(c.shutdownChan)
		}()
	}
	<-c.shutdownChan
}
