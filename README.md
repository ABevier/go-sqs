# go-sqs

Amazon SQS is charged by the api request, not by the message.  In order to maximize performance and minimize cost Long Polling and Batching should be used.
The goal of this library is to make correct usage of SQS easy by abstracting away the batching and long polling behind a simple api

## Options

Max Linger - The maximum amount of time a Send Message or Delete Message request will be queued in a batch before the batch is sent

## Usage

### Batching
SQS provides a batch API that allows up to 10 SendMessage or 10 DeleteMessage requests to be issued in a single API call.

This library automatically batches requests.  Batches are sent when they have 10 messages or when the batch is older than the configured linger time.

### Long Polling
Long polling for Retrieve Message requests keeps a connection open for up to 20 seconds waiting for messages to become visible in the SQS queue.
This library uses a variable number of concurrent Retrieve Message requests based on the load of the queue.

The library starts by issuing a single Retrieve Message request.  If the number of messages returned by the request is above the specified threshold (default 7)
then the maximum allowed conncurent requests will be increased by 1, to a configurable maximum.  If the number of messages returned is below the specified threshold
(default 3) then the number of concurrent requests will be decreased by 1, to a minimum of 1.  This has the effect of elastically scaling the number of
requests based on the volume of messages in the queue.


TODO
- Automated retries with exponential backoff
- Callback handler for failed ACKs?
- Metrics?
