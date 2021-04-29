package main

import (
	"context"
	"fmt"
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

	fmt.Printf("Queue URL= . %v", result)
	sqsQueue := NewSqsQueue(client, result.QueueUrl, 3*time.Second)

	consumer := NewConsumer(sqsQueue)
	consumer.Start()
	consumeMessages(consumer)
	consumer.Shutdown()

	// for i := 0; i < 14; i++ {
	// 	go func(idx int) {
	// 		result, err := sqsQueue.SendMessage("msg:" + strconv.Itoa(idx))
	// 		if err != nil {
	// 			fmt.Print(err)
	// 		} else {
	// 			fmt.Printf("result=%v\n", result)
	// 		}
	// 	}(i)
	// }

	time.Sleep(10 * time.Second)
}

func consumeMessages(consumer *SqsQueueConsumer) {
	timer := time.NewTimer(10 * time.Second)

	for {
		select {
		case msg := <-consumer.MessageChan:
			fmt.Printf("GOT MESSAGE: %v\n", msg)

		case <-timer.C:
			fmt.Println("timer tick")
			return
		}
	}
}
