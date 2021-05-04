package internal

const MINIMUM_RETRIEVE_REQUEST_LIMIT = 1

//TODO: make a struct
const MAX_PREFETCHED_MESSAGES = 100
const MAX_OUTSTANDING_FETCH_REQUESTS = 10

func calculateNeededRetrieveRequests(messageCount, inflightRequests, currentRetrieveRequestLimit int) int {
	// The "potential" number of messages  = already fetched + what could possibly come back from inflight requests
	messagePotential := (inflightRequests * MAX_BATCH) + messageCount

	// The number or requests needed to hit the maximum allowed unacknowledged message count
	requestsForMaxMessages := (MAX_PREFETCHED_MESSAGES - messagePotential) / MAX_BATCH

	// The remaining slots for concurrent retrieve requests
	remainingRequestSlots := currentRetrieveRequestLimit - inflightRequests

	return min(requestsForMaxMessages, remainingRequestSlots)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func calculateNewRetrieveRequestLimit(currentRetrieveRequestLimit, lastRetrievedCount int) int {
	adjustment := calculateAdjustment(lastRetrievedCount)
	desiredNewLimit := currentRetrieveRequestLimit + adjustment

	return constrain(desiredNewLimit, MINIMUM_RETRIEVE_REQUEST_LIMIT, MAX_OUTSTANDING_FETCH_REQUESTS)
}

//TODO: make this configurable
func calculateAdjustment(lastRetrievedCount int) int {
	if lastRetrievedCount <= 3 {
		return -1
	} else if lastRetrievedCount >= 7 {
		return 1
	} else {
		return 0
	}
}

func constrain(value, lower, upper int) int {
	if value < lower {
		return lower
	} else if value > upper {
		return upper
	}
	return value
}
