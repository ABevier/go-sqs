package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"
)

func main() {

	//
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("config error")
	}
	client := sqs.NewFromConfig(cfg)

	queueName := "abevier_test_queue"
	input := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}

	result, err := client.GetQueueUrl(context.TODO(), input)
	if err != nil {
		fmt.Println("Err on get QueueUrl")
		fmt.Println(err)
		return
	}

	fmt.Printf("Queue URL= . %v\n", *result.QueueUrl)
	sqsQueue := gosqs.NewPublisher(client, *result.QueueUrl, 3*time.Second)

	numMessages := 1000
	wg := sync.WaitGroup{}
	wg.Add(numMessages)

	for i := 0; i < numMessages; i++ {
		go func(idx int) {
			defer wg.Done()
			ctx := context.Background()
			result, err := sqsQueue.SendMessage(ctx, "msg:"+strconv.Itoa(idx))
			if err != nil {
				fmt.Print(err)
			} else {
				fmt.Printf("send msg %v and had id=%v\n", idx, result)
			}
		}(i)
	}

	fmt.Println("waiting for sending to complete")
	wg.Wait()
	fmt.Println("done waiting")

	var count int64

	opts := gosqs.Opts{MaxReceivedMessages: 100, MaxWorkers: 3, MaxInflightReceiveMessageRequests: 10}
	consumer := gosqs.NewConsumer(opts, sqsQueue, func(ctx context.Context, message string) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	consumer.Start()
	time.Sleep(60 * time.Second)
	consumer.Shutdown()

	fmt.Printf("it's shut down after reading %v messages\n", count)

	time.Sleep(10 * time.Second)
}
