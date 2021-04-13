package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	. "github.com/abevier/go-sqs/internal"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {

	printBatch := func(b Batch) {
		for _, value := range b.Buffer {
			fmt.Println(value)
		}
	}

	be := NewBatchExecutor(printBatch)

	for i := 0; i < 44; i++ {
		be.AddItem(strconv.Itoa(i))
	}

	time.Sleep(10 * time.Second)

	//
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("config error")
	}

	client := sqs.NewFromConfig(cfg)

	queueName := "alan-test-queue"
	input := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}

	result, err := client.GetQueueUrl(context.TODO(), input)
	if err != nil {
		fmt.Println("Err on get QueueUrl")
		fmt.Println(err)
		return
	}

	fmt.Printf("Queue URL= . %v", result)
}
