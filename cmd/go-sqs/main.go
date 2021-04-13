package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {

	printBatch := func(b batch) {
		for _, value := range b.buffer {
			fmt.Println(value)
		}
	}

	exec := &batchExecutor{
		sequenceNumber: 0,
		m:              &sync.Mutex{},
		executeF:       printBatch,
	}

	for i := 0; i < 44; i++ {
		addItem(exec, strconv.Itoa(i))
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

type batchExecutor = struct {
	m              *sync.Mutex
	sequenceNumber uint
	currentBatch   *batch
	executeF       func(b batch)
}

type batch = struct {
	batchId uint
	buffer  []string
}

func addItem(b *batchExecutor, msg string) {
	var readyBatch *batch = nil

	b.m.Lock()
	if b.currentBatch == nil {
		b.currentBatch = newBatch(b)
	}
	b.currentBatch.buffer = append(b.currentBatch.buffer, msg)

	fmt.Printf("batch count = %v \n", len(b.currentBatch.buffer))

	if len(b.currentBatch.buffer) >= 10 {
		readyBatch = b.currentBatch
		b.currentBatch = nil
	}
	b.m.Unlock()

	// be sure to execute the batch outside of the lock
	if readyBatch != nil {
		fmt.Println("FLUSHED DUE TO MAX SIZE")
		b.executeF(*readyBatch)
	}
}

func newBatch(be *batchExecutor) *batch {
	//Assumes a lock on the executor
	be.sequenceNumber++

	result := &batch{
		batchId: be.sequenceNumber,
		buffer:  []string{},
	}

	go expireBatch(be, be.sequenceNumber)

	return result
}

func expireBatch(be *batchExecutor, batchId uint) {
	var readyBatch *batch = nil

	time.Sleep(3 * time.Second)

	be.m.Lock()
	fmt.Printf("Check Batch with id: %v\n", batchId)
	if be.currentBatch != nil && be.currentBatch.batchId == batchId {
		readyBatch = be.currentBatch
		be.currentBatch = nil
	}
	be.m.Unlock()

	// be sure to execute the batch outside of the lock
	if readyBatch != nil {
		fmt.Println("EXPIRED BATCH")
		be.executeF(*readyBatch)
	}
}
