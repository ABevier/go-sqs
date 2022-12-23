package gosqs

// minReceiveRequestLimit is the minimum number of outstanding SQS Receieve messages requests the system will use
// this should always be 1
const minOutstandingReceiveRequests = 1

type calculator struct {
	maxReceivedMessages           int
	maxOutstandingReceiveRequests int
}

func newCalculator(maxReceivedMessages, maxOutstandingReceiveRequests int) *calculator {
	return &calculator{
		maxReceivedMessages:           maxReceivedMessages,
		maxOutstandingReceiveRequests: maxOutstandingReceiveRequests,
	}
}

func (c *calculator) NeededReceiveRequests(messageCount, inflightRequests, currentReceiveRequestLimit int) int {
	// The "potential" number of messages  = already received + what could possibly come back from inflight requests
	messagePotential := (inflightRequests * MaxSQSBatch) + messageCount

	// The number or requests needed to hit the maximum allowed unacknowledged message count
	requestsForMaxMessages := (c.maxReceivedMessages - messagePotential) / MaxSQSBatch

	// The remaining slots for concurrent retrieve requests
	remainingReceiveRequest := currentReceiveRequestLimit - inflightRequests

	return min(requestsForMaxMessages, remainingReceiveRequest)
}

func (c *calculator) NewReceiveRequestLimit(currentReceiveRequestLimit, lastReceivedMessageCount int) int {
	desiredNewLimit := currentReceiveRequestLimit + c.calcAdjustment(lastReceivedMessageCount)
	return constrain(desiredNewLimit, minOutstandingReceiveRequests, c.maxOutstandingReceiveRequests)
}

func (c *calculator) calcAdjustment(lastRetrievedCount int) int {
	if lastRetrievedCount <= 3 {
		return -1
	} else if lastRetrievedCount >= 7 {
		return 1
	} else {
		return 0
	}
}
