package internal

import (
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

func NewBatchExecutor(maxLinger time.Duration, executeF func(b *Batch)) *BatchExecutor {
	return &BatchExecutor{
		sequenceNumber: 0,
		m:              &sync.Mutex{},
		executeF:       executeF,
		maxLinger:      maxLinger,
	}
}

type Batch struct {
	batchId uint
	Buffer  []interface{}
}

func (b *BatchExecutor) AddItem(item interface{}) {
	b.m.Lock()
	if b.currentBatch == nil {
		b.currentBatch = newBatch(b)
	}
	b.currentBatch.Buffer = append(b.currentBatch.Buffer, item)

	if len(b.currentBatch.Buffer) >= 10 {
		go b.executeF(b.currentBatch)
		b.currentBatch = nil
	}
	b.m.Unlock()
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
	time.Sleep(be.maxLinger)

	be.m.Lock()
	if be.currentBatch != nil && be.currentBatch.batchId == batchId {
		go be.executeF(be.currentBatch)
		be.currentBatch = nil
	}
	be.m.Unlock()
}
