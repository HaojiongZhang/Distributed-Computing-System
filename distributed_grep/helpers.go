package main

import (
	"net/rpc"
	"strings"
	"sync"
)

const (
	Network = "tcp4"
	Port    = ":8080"
)

type LogQueryServer string

type LogQueryArgs struct {
	Flag    string
	Pattern string
}

type LogQueryOutput struct {
	Output           string
	MatchedLineCount int
}

// Helper function for sender
// Queries all the connect servers for grep using go routines
// Return the grep result and total # of lines returned
func QueryAllServers(clientList []*rpc.Client, queryArgs LogQueryArgs) (int, string) {
	var totalOutput strings.Builder
	totalMatchedLineCount := 0

	grepResults := [10]LogQueryOutput{}

	var wg sync.WaitGroup

	for i, client := range clientList {
		wg.Add(1)
		go func(c *rpc.Client, i int) {
			defer wg.Done()
			err := c.Call("LogQueryServer.GetLogQueryResult", queryArgs, &(grepResults[i]))
			if err != nil {
				return
			}
		}(client, i)
	}

	wg.Wait() // This will block until all goroutines decrement the WaitGroup counter to 0

	for _, reply := range grepResults[:10] {
		totalMatchedLineCount += reply.MatchedLineCount
		totalOutput.WriteString(reply.Output)
	}

	return totalMatchedLineCount, totalOutput.String()

}
