package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// Helper function for getLogPath
// Returns current directory of the program
func getSourceDir() string {
	_, currentFile, _, _ := runtime.Caller(0)
	return filepath.Dir(currentFile)
}

// Get log file name based on vm hostname
func getLogPath() string {
	hostnameOutput, err := exec.Command("hostname").Output()

	if err != nil {
		fmt.Println("Error when fetching host name!")
		return ""
	}

	hostname := string(hostnameOutput)

	// Extract vm number from host name (fa23-cs425-32##.cs.illinois.edu)
	index := strings.Index(hostname, ".")

	// If '.' doesnt exist or is in the first two characters
	if index < 2 {
		fmt.Println("Unable to find machine number from host name!")
		return ""
	}

	num, err := strconv.Atoi(hostname[index-2 : index])
	if err != nil {
		fmt.Printf("Error converting string to int: %s\n", err)
	}

	return fmt.Sprintf("../machine.%d.log", num)
}

// Apply Grep on the machine log file
// Return grep result and matched line count
func (t *LogQueryServer) GetLogQueryResult(args LogQueryArgs, logQueryOutput *LogQueryOutput) error {
	fmt.Println("Computing Log Query output for grep ", args.Flag, " ", args.Pattern)

	logFileName := getLogPath()

	grepResult := exec.Command("grep", args.Flag, args.Pattern, logFileName)
	stdOutput, error := grepResult.Output()

	if error != nil {
		fmt.Println("Failed to grep ", logFileName)
		return error
	}

	// Split into matched lines
	outputLines := strings.Split(string(stdOutput), "\n")
	logQueryOutput.MatchedLineCount = 0
	var strBuilder strings.Builder

	// Add output message for each log entry line matched with log filename
	for _, line := range outputLines {
		if len(line) != 0 {
			strBuilder.WriteString(fmt.Sprintf("%s: %s\n", logFileName, line))
			logQueryOutput.MatchedLineCount += 1
		}
	}

	logQueryOutput.Output = strBuilder.String()

	return nil
}

// listener to server RPC connections
func listener() {
	server := new(LogQueryServer)
	error := rpc.Register(server)

	if error != nil {
		fmt.Println("Failed to register server")
		return
	}

	listener, error := net.Listen(Network, Port)

	if error != nil {
		fmt.Println("Failed to listen on port", Port)
		return
	}
	fmt.Println("Listening on port", Port)

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		fmt.Println("trying to listen")
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)

	}
}

// Helper Function for Sender
// Verifies that user entered grep in the correct format
func readGrepCommand(reader *bufio.Reader) (string, string, error) {
	fmt.Print("Enter the grep command: ")
	grepCommand, _ := reader.ReadString('\n')
	tokens := strings.Fields(grepCommand)

	if len(tokens) < 3 || tokens[0] != "grep" {
		return "", "", errors.New("Invalid command. Must start with 'grep'")
	}

	options := tokens[1]

	_, err := regexp.Compile(options)
	if err != nil {
		return "", "", errors.New("Options are not a valid regular expression")
	}

	pattern := tokens[2]

	return options, pattern, nil
}

// Gracefully closes TCP connection
func closeConnections(clientList []*rpc.Client) {
	for _, client := range clientList {
		client.Close()
	}
}

func parser(input string) int {
	total := 0
	lines := strings.Split(input, "\n")
	for _, line := range lines {
		parts := strings.Split(line, ":")
		if len(parts) == 2 {
			// Extract the numeric part after ":"
			valueStr := strings.TrimSpace(parts[1])
			value, err := strconv.Atoi(valueStr)
			if err == nil {
				total += value
			}
		}
	}
	return total
}

// Sender function that connects to every client, continously read grep from user, and query from connect machines
func sender() {
	reader := bufio.NewReader(os.Stdin)
	var clientList []*rpc.Client
	initConnect := true

	for true {
		// Read grep input
		options, pattern, err := readGrepCommand(reader)

		if err != nil {

			fmt.Println("Incorrect Grep")
			closeConnections(clientList)
			fmt.Println("=====================Program Terminated=====================")
			return
		}

		queryArgs := LogQueryArgs{
			Flag:    options,
			Pattern: pattern,
		}

		// Connect to all servers
		if initConnect {
			fmt.Println("Trying to connect to server ...")

			for i := 3201; i <= 3210; i++ {
				hostname := fmt.Sprintf("fa23-cs425-%d.cs.illinois.edu", i)
				client, err := rpc.Dial(Network, hostname+Port)
				if err != nil {
					fmt.Printf("Failed to connect to Server (%s): %v\n", hostname, err)
					continue // skip this iteration if there's an error
				}
				fmt.Printf("Connected to server %s\n", hostname)
				clientList = append(clientList, client)
			}
			fmt.Printf("Connected to %d server\n", len(clientList))
			initConnect = false
		}

		// Get results
		totalMatchedLineCount, totalOutput := QueryAllServers(clientList, queryArgs)

		// Print the tallied results
		if strings.Contains(options, "c") {
			totalMatchedLineCount = parser(totalOutput)
		}
		fmt.Printf("\n%s", totalOutput)
		fmt.Printf("\nTotal matched lines: %d\n\n", totalMatchedLineCount)
	}
}

// call the function by [go run main.go helpers.go]
func main() {

	go listener()
	sender()

}
