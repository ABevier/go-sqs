package gosqs

func constrain(value, lower, upper int) int {
	if value < lower {
		return lower
	} else if value > upper {
		return upper
	}
	return value
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
