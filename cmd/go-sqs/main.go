package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	. "github.com/abevier/go-sqs/internal"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
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

	fmt.Printf("Queue URL= . %v\n", result.QueueUrl)
	sqsQueue := NewSqsQueue(client, result.QueueUrl, 3*time.Second)

	numMessages := 10
	wg := sync.WaitGroup{}
	wg.Add(numMessages)

	for i := 0; i < numMessages; i++ {
		go func(idx int) {
			defer wg.Done()
			result, err := sqsQueue.SendMessage("msg:" + strconv.Itoa(idx))
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

	consumer := NewConsumer(sqsQueue, 20, 10, consumeMessageCallback)
	consumer.Start() // Give this a callback
	time.Sleep(20 * time.Second)
	consumer.Shutdown()

	fmt.Printf("it's shut down")
	time.Sleep(10 * time.Second)
}

func consumeMessageCallback(message *string) {
	fmt.Printf("GOT MESSAGE: %v\n", *message)
}
