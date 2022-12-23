package gosqs

type Opts struct {
	// MaxReceiveMessages is the maximum number of messages that will be pulled from SQS without being ack'd.  This
	// includes both messages being processed and messages queued for processing
	MaxReceivedMessages int
	// MaxWorkers is the maximum number of messages that will concurently be processed
	MaxWorkers int
	// MaxInflightReceieveMessageRequests is the upper limit on the number of outstanding receieve message requests.
	MaxInflightReceiveMessageRequests int
}
