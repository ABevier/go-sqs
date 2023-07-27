package gosqs

import (
	"context"
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

	workerWG              *sync.WaitGroup
	isShutdown            uint32
	isRxShutdown          uint32
	shutdownInitiatedChan chan struct{}
	shutdownChan          chan struct{}
	rxShutdownChan        chan struct{}
}

func NewConsumer(opts Opts, publisher *SQSPublisher, callback MessageCallbackFunc) *SQSConsumer {
	return &SQSConsumer{
		publisher: publisher,

		maxReceivedMessages:               opts.MaxReceivedMessages,
		maxWorkers:                        opts.MaxWorkers,
		maxInflightReceiveMessageRequests: opts.MaxInflightReceiveMessageRequests,

		callbackFunc: callback,

		workerWG:              &sync.WaitGroup{},
		isShutdown:            0,
		isRxShutdown:          0,
		shutdownInitiatedChan: make(chan struct{}),
		shutdownChan:          make(chan struct{}),
		rxShutdownChan:        make(chan struct{}),
	}
}

func (c *SQSConsumer) Start() {
	msgProcessingCompleteChannel := make(chan struct{})

	messageChan := make(chan SQSMessage, c.maxReceivedMessages)

	// number of outbound requests.  min of 1 to max of config value
	// do a request
	// count number of messages pulled
	// if greater than 7 - make 2 requests
	// if less than 3 - do not make a request another request (unless 0 requests would be outstanding)
	// also check this against the number I'm allowed to prefetch

	go func() {
		retrievedMsgChan := make(chan []SQSMessage)

		numReceivedMessages := 0
		numInflightRetrieveRequests := 0
		retrieveRequestLimit := minOutstandingReceiveRequests

		calc := newCalculator(c.maxReceivedMessages, c.maxInflightReceiveMessageRequests)

		for {
			if atomic.LoadUint32(&c.isShutdown) == 1 {
				if numInflightRetrieveRequests == 0 {
					// Close the rx shutdown channel to signal that all SQS receive requests have completed. Note that
					// the processing for these requests, or for the messages returned for the requests, could still be
					// in progress after the channel is closed.
					if atomic.CompareAndSwapUint32(&c.isRxShutdown, 0, 1) {
						close(c.rxShutdownChan)
					}
					if numReceivedMessages == 0 {
						break
					}
				}
			} else {
				neededRequests := calc.NeededReceiveRequests(numReceivedMessages, numInflightRetrieveRequests, retrieveRequestLimit)

				// fmt.Printf("Consumer State: msgCnt: %v retrieveCnt: %v retrieveLimit: %v needed: %v \n",
				// 	numMessages, numInflightRetrieveRequests, retrieveRequestLimit, neededRequests)

				for i := 0; i < neededRequests; i++ {
					go func() {
						receiveMessageWorker(c.publisher, retrievedMsgChan)
					}()
				}
				numInflightRetrieveRequests += neededRequests
			}

			select {
			case msgs := <-retrievedMsgChan:
				numInflightRetrieveRequests--
				numReceivedMessages += len(msgs)
				retrieveRequestLimit = calc.NewReceiveRequestLimit(retrieveRequestLimit, len(msgs))
				for _, m := range msgs {
					messageChan <- m
				}

			case <-msgProcessingCompleteChannel:
				numReceivedMessages--
			case _, ok := <-c.shutdownInitiatedChan:
				if ok {
					log.Printf("gosqs: initiating shutdown...")
				}
				// TODO: Do something else if the channel was already closed?
			}
		}

		// All writers to these channels should have completed by the time the above loop exits
		close(retrievedMsgChan)
		close(messageChan)
	}()

	// Start a bunch of workers
	for i := 0; i < c.maxWorkers; i++ {
		c.workerWG.Add(1)
		go func(id int) {
			defer c.workerWG.Done()

			for msg := range messageChan {
				// TODO: disabling for now, will add back with better logging level support
				//log.Printf("Handling message: %v on worker: %v\n", msg.body, id)
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
		log.Printf("gosqs: err acking - what do? %v", err)
	}
}

func (c *SQSConsumer) Shutdown() {
	if atomic.CompareAndSwapUint32(&c.isShutdown, 0, 1) {
		c.shutdownInitiatedChan <- struct{}{}
		go func() {
			c.workerWG.Wait()
			close(c.shutdownChan)
		}()
	}
	<-c.shutdownChan
	log.Printf("gosqs: shutdown complete")
}

func (c *SQSConsumer) WaitForRxShutdown() {
	// This channel is only used to detect the close of SQS receive operations and signal the app that there are no more
	// messages forthcoming beyond the ones currently in flight.
	log.Printf("gosqs: waiting for rx shutdown...")
	<-c.rxShutdownChan
	log.Printf("gosqs: rx shutdown complete")
}
