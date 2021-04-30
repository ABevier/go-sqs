package internal

const MINIMUM_FETCH_REQUEST_LIMIT = 1

//TODO: make a struct
const MAX_PREFETCHED_MESSAGES = 100
const MAX_OUTSTANDING_FETCH_REQUESTS = 10

func calculateNeededFetches(messageCount, inflightRequests, currentFetchRequestLimit int) int {
	// The "potential" number of messages  = already fetched + what could possibly come back from inflight requests
	messagePotential := (inflightRequests * MAX_BATCH) + messageCount

	// The number or requests needed to hit the maximum allowed unacknowledged message count
	requestsForMaxMessages := (MAX_PREFETCHED_MESSAGES - messagePotential) / MAX_BATCH

	// The remaining slots for concurrent fetch requests
	remainingRequestSlots := currentFetchRequestLimit - inflightRequests

	return min(requestsForMaxMessages, remainingRequestSlots)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func calculateNewFetchRequestLimit(currentFetchLimit, lastFetchedCount int) int {
	adjustment := calculateAdjustment(lastFetchedCount)
	desiredNewLimit := currentFetchLimit + adjustment

	return constrain(desiredNewLimit, MINIMUM_FETCH_REQUEST_LIMIT, MAX_OUTSTANDING_FETCH_REQUESTS)
}

//TODO: make this configurable
func calculateAdjustment(lastFetchedCount int) int {
	if lastFetchedCount <= 3 {
		return -1
	} else if lastFetchedCount >= 7 {
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
