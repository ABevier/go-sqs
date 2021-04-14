package internal

import (
	"fmt"
	"sync"
	"time"
)

type BatchExecutor struct {
	m              *sync.Mutex
	sequenceNumber uint
	currentBatch   *Batch
	executeF       func(b *Batch)
	maxLinger      time.Duration
}

func NewBatchExecutor(executeF func(b *Batch)) *BatchExecutor {
	return &BatchExecutor{
		sequenceNumber: 0,
		m:              &sync.Mutex{},
		executeF:       executeF,
		maxLinger:      3 * time.Second,
	}
}

type Batch struct {
	batchId uint
	Buffer  []interface{}
}

func (b *BatchExecutor) AddItem(item interface{}) {
	var readyBatch *Batch = nil

	b.m.Lock()
	if b.currentBatch == nil {
		b.currentBatch = newBatch(b)
	}
	b.currentBatch.Buffer = append(b.currentBatch.Buffer, item)

	fmt.Printf("batch count = %v \n", len(b.currentBatch.Buffer))

	if len(b.currentBatch.Buffer) >= 10 {
		readyBatch = b.currentBatch
		b.currentBatch = nil
	}
	b.m.Unlock()

	// be sure to execute the batch outside of the lock
	if readyBatch != nil {
		fmt.Println("FLUSHED DUE TO MAX SIZE")
		b.executeF(readyBatch)
	}
}

func newBatch(be *BatchExecutor) *Batch {
	//Assumes a lock on the executor
	be.sequenceNumber++

	result := &Batch{
		batchId: be.sequenceNumber,
		Buffer:  make([]interface{}, 0, 10),
	}

	go expireBatch(be, be.sequenceNumber)

	return result
}

func expireBatch(be *BatchExecutor, batchId uint) {
	var readyBatch *Batch = nil

	time.Sleep(be.maxLinger)

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
		be.executeF(readyBatch)
	}
}
