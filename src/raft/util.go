package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func max(x, y int) int {
	if(x > y) {
		return x
	}
	return y
}

func min(x, y int) int {
	if(x > y) {
		return y
	}
	return x
}