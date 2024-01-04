package main

import (
	"fmt"
	"net/rpc"
	"os/exec"
	"strings"
)

type TestCase struct {
	Case    string
	Pattern string
}

const (
	Flag = "-e"
)

// Generate distributed grep result with all active server
func distributedGrep(pattern string, result *LogQueryOutput, activeVmNumbers *[]int) error {
	// Set up arguments
	queryArgs := LogQueryArgs{
		Flag:    Flag,
		Pattern: pattern,
	}

	// Connect to all servers
	var clientList []*rpc.Client
	for i := 3201; i <= 3210; i++ {
		hostname := fmt.Sprintf("fa23-cs425-%d.cs.illinois.edu", i)
		client, err := rpc.Dial(Network, hostname+Port)
		if err != nil {
			// If this server didn't connect, test with the remaining server
			continue
		}
		clientList = append(clientList, client)
		// Append vm number which will correspond to machine.#.log
		*activeVmNumbers = append(*activeVmNumbers, i-3200)
	}

	// Test distributed grep query
	totalMatchedLineCount, totalOutput := QueryAllServers(clientList, queryArgs)

	result.Output = totalOutput
	result.MatchedLineCount = totalMatchedLineCount

	return nil
}

// Generate local grep result of log files corresponding to all active server
func localGrep(pattern string, result *LogQueryOutput, activeVmNumbers []int) error {
	var strBuilder strings.Builder

	for _, vmNum := range activeVmNumbers {
		fileName := fmt.Sprintf("machine.%d.log", vmNum)
		grepResult := exec.Command("grep", Flag, pattern, fileName)
		stdOutput, error := grepResult.Output()

		// Could be error or empty response, log and continue to return default output
		if error != nil {
			fmt.Printf("Failed to grep %s %s %s\n", Flag, pattern, fileName)
		}

		// Split into matched lines
		outputLines := strings.Split(string(stdOutput), "\n")

		// Add output message for each log entry line matched with log filename
		for _, line := range outputLines {
			if len(line) != 0 {
				strBuilder.WriteString(fmt.Sprintf("%s: %s\n", fileName, line))
				result.MatchedLineCount += 1
			}
		}
	}
	result.Output = strBuilder.String()

	return nil
}

// Given a pattern, compare grep output between distributed and local
func validate(pattern string) bool {
	var distributedGrepResult LogQueryOutput
	var localGrepResult LogQueryOutput
	// Used to track which servers are active so local grep uses the correct log files
	var activeVmNumbers []int

	distributedErr := distributedGrep(pattern, &distributedGrepResult, &activeVmNumbers)

	// If there is an error, the test fails
	if distributedErr != nil {
		fmt.Println(distributedErr)
		return false
	}

	localErr := localGrep(pattern, &localGrepResult, activeVmNumbers)

	// If there is an error, the test fails
	if localErr != nil {
		fmt.Println(localErr)
		return false
	}

	fmt.Println("\n[Distributed Grep Results]")
	fmt.Printf("\n%s", distributedGrepResult.Output)
	fmt.Printf("Total matched lines: %d\n\n", distributedGrepResult.MatchedLineCount)

	fmt.Println("\n[Local Grep Results]")
	fmt.Printf("\n%s", localGrepResult.Output)
	fmt.Printf("Total matched lines: %d\n\n", localGrepResult.MatchedLineCount)

	// Check if results match
	if distributedGrepResult.Output == localGrepResult.Output && distributedGrepResult.MatchedLineCount == localGrepResult.MatchedLineCount {
		return true
	} else {
		return false
	}
}

// call the function by [go run UnitTest.go helpers.go]
func main() {
	var testCases [6]TestCase

	testCases[0] = TestCase{"Rare", "[Ww]hispers"}
	testCases[1] = TestCase{"Frequent", "[Ss]tars"}
	testCases[2] = TestCase{"Somewhat Frequent", "[Dd]reams"}
	testCases[3] = TestCase{"Occurs in One Log", "1[|.]2[|.]3[|.]4"}
	testCases[4] = TestCase{"Occurs in Some Logs", "By[ ]*yours truly"}
	testCases[5] = TestCase{"Occurs in all Logs", "Pickle Man."}

	for _, testCase := range testCases {
		fmt.Println("-------------------------")
		fmt.Printf("TestCase: %s \t Pattern: %s\n", testCase.Case, testCase.Pattern)
		hasPassed := validate(testCase.Pattern)

		if !hasPassed {
			fmt.Println("Test failed")
			return
		} else {
			fmt.Println("Test passed")
		}
	}
	fmt.Println("-------------------------")
	fmt.Println("All tests passed")
	fmt.Println("-------------------------")
}
