package main

import (
	"bufio"
	// "errors"
	
	"fmt"
	"net"
	"os"
	"strings"
	"flag"
	"time"
	"sync"
	"encoding/json"
	"math/rand"
	"log"
	"strconv"
	
	"regexp"
	"io/ioutil"
)

var(
	clientListMutex sync.Mutex
	transmitMutex sync.Mutex
	ClientList []ClientInfo
	IsSusEnabled bool
	CommandMode int
	logger *log.Logger
	logFile *os.File
	isPaused bool
	isVerbose bool
	byteUsage int
	falsePositiveCount int // only makes sense if no nodes leave/crash
)
var startChan = make(chan bool)
var stopChan = make(chan bool)
var fileTimestamps = make(map[string]time.Time)

type FileData struct {
	Filename    string `json:"filename"`
	Timestamp   time.Time `json:"timestamp"`
	FileContent string `json:"filecontent"`
}

const (
	Network = "udp"
	IntroPort  = ":4444"
	UDPPort = ":8888"
	FileCmdPort = ":5555"
	ReplicatePort = ":7777"
	MultireadPort = ":2222"
	FilenameDelimiter = '\n'
	IntroHostname = "fa23-cs425-3207.cs.illinois.edu"
	NORM = 0
	SUSP = 1
	FAIL = 2
	Tfail = 4 * time.Second
	TFailWithSus = 3500 * time.Millisecond
	TSus = 3000 * time.Millisecond
	TCleanup = 5000 * time.Millisecond
	GET = "get"
	PUT = "put"
	DELETE = "delete"
	FileFolder = "storage/"
)

// Commands
const (
	NOTHING = 0
	GOSSIP = 1
	GOSSIP_SUS = 2
)

type ClientInfo struct {
	Hostname string
	Port     string
	HeartBeatCount int
	TimeStamp time.Time
	SFlag int            
}

// Carry flag for switching between gossip & gossip + s
type Message struct {
	Command int
	ClientList []ClientInfo
}

//Helper function for merging Clientlist
func isClientInfoDuplicate(hostname string) bool {
    for _, clientInfo := range ClientList {
        if clientInfo.Hostname == hostname {
            return true // A client with the same hostname already exists
        }
    }
    return false 
}

//Helper function to compare two Clientlists
func clientListsEqual(a, b []ClientInfo) bool {
    if len(a) != len(b) {
        return false
    }

    for _, clientA := range a {
        if !contains(b, clientA) {
            return false
        }
    }

    return true
}

//Helper function for checking if a client is in the ClientList
func contains(clients []ClientInfo, client ClientInfo) bool {
    for _, c := range clients {
        if c.Hostname == client.Hostname {
            return true
        }
    }
    return false
}

//Helper function for merging Clientlist
func addClientInfo(clientInfo ClientInfo){
	clientListMutex.Lock()
    defer clientListMutex.Unlock()

    // Check for duplicates before adding
    if !isClientInfoDuplicate(clientInfo.Hostname) {
        // No duplicate found, add the clientInfo
        ClientList = append(ClientList, clientInfo)

		if isVerbose {
			fmt.Printf("Added ClientInfo: %+v\n", clientInfo)
		}
    } else {
		if isVerbose {
        	fmt.Printf("ClientInfo with hostname %s already exists, not adding.\n", clientInfo.Hostname)
		}
    }
}

//Introducer runs on TCP and connects the new node to the current clientlist
func introducer() {
    tcpAddr, err := net.ResolveTCPAddr("tcp4", IntroHostname+IntroPort)
    if err != nil {
        fmt.Println("Error resolving address:", err)
        return
    }

 
    tcpListener, err := net.ListenTCP("tcp4", tcpAddr)
    if err != nil {
        fmt.Println("Error creating UDP connection:", err)
        return
    }
    defer tcpListener.Close()

    fmt.Println("Introducer is listening on", IntroHostname)



    for {
        
        conn, err := tcpListener.Accept()
        if err != nil {
            fmt.Println("Error accepting incoming connection:", err)
            return
        }

        go func(c net.Conn) {
			defer c.Close()
	
			buffer := make([]byte, 1024)
			n, err := c.Read(buffer)
			if err != nil {
				fmt.Println("Error reading from client:", err)
				return
			}
	
			var clientInfo ClientInfo
			if err := json.Unmarshal(buffer[:n], &clientInfo); err != nil {
				fmt.Println("Error decoding JSON data:", err)
				return
			}
	
			addClientInfo(clientInfo)
			logger.Printf("New node %s joined at time %s ", clientInfo.Hostname, time.Now())
			if isVerbose {			
				fmt.Printf("Received ClientInfo from %s: %+v\n", c.RemoteAddr(), clientInfo)
			}
	
			jsonData, err := json.Marshal(ClientList)
			if err != nil {
				fmt.Println("Error encoding ClientList to JSON:", err)
				return
			}
	
			_, err = c.Write(jsonData)
			if err != nil {
				fmt.Println("Error sending ClientList to client:", err)
			}
		}(conn)
	
    }
	
}

//Helper function for listener, merge incoming clientlist with local client list
func mergeClientList(newClientList []ClientInfo, gossipTime time.Time){
	clientListMutex.Lock()
    defer clientListMutex.Unlock()

	myHostname,_ := os.Hostname()
	// Using map to store the combined list with Hostname:Port as key
	clientMap := make(map[string]ClientInfo)

	// Insert/update local list items to the map
	for _, client := range ClientList {
		key := client.Hostname + ":" + client.Port
		clientMap[key] = client
	}
	
	// Insert/update new list items to the map
	for _, client := range newClientList {
		key := client.Hostname + ":" + client.Port
		if client.Hostname == myHostname{
			continue
		}
		// If the client exists in the map, compare HeartBeatCount
		if existingClient, exists := clientMap[key]; exists {
			// Update if the heart beat is greater
			// Failed nodes can't be revived
			if client.HeartBeatCount > existingClient.HeartBeatCount && existingClient.SFlag != FAIL {             // > or >= ???
				// Log if a suspicious node is revived
				if client.SFlag == NORM && existingClient.SFlag == SUSP{
					logger.Printf("Suspicious Node %s is marked as alive at time %s ", client.Hostname, time.Now())
				} else if client.SFlag == FAIL {
					logger.Printf("Node %s is gossiped as Failed at time %s ", client.Hostname, time.Now())
				}
				client.TimeStamp = gossipTime
				clientMap[key] = client
			} 
		} else {
			
			//To-do: add a logger mutex

			// Log the status of the newly gossiped node
			statusMessage := ""

			switch client.SFlag {
				case NORM:
					statusMessage = "normal"
				case FAIL:
					continue
				case SUSP:
					statusMessage = "suspicious"
				default:
					fmt.Println("Client status flag unknown")
			}
			client.TimeStamp = gossipTime
			clientMap[key] = client
			logger.Printf("New %s node %s gossiped at time %s ", statusMessage, client.Hostname, time.Now())
		}
	}

	// Convert the map back to a slice
	var mergedList []ClientInfo
	for _, client := range clientMap {
		mergedList = append(mergedList, client)
	}

	ClientList = mergedList

	if isVerbose {
		fmt.Println("New updated Member list: ", time.Now())
	}
	printClientList()
}

//Run when first a node first joins
//Sends it's own info and receives updated clientlist from introducer
func pingIntroducer(){
	jsonData, err := json.Marshal(ClientList[0])
    if err != nil {
        fmt.Println("Error encoding ClientList to JSON:", err)
        return
    }

    // Resolve the TCP address
    tcpAddr, err := net.ResolveTCPAddr("tcp4", IntroHostname+IntroPort)
    if err != nil {
        fmt.Println("Error resolving address:", err)
        return
    }

    // Create a TCP connection to send data
    conn, err := net.DialTCP("tcp4", nil, tcpAddr)
    if err != nil {
        fmt.Println("Error creating TCP connection:", err)
        return
    }
    defer conn.Close()

    // Send ClientInfo
    _, err = conn.Write(jsonData)
    if err != nil {
        fmt.Println("Error sending ClientInfo to introducer:", err)
        return
    }

    // Read the response (updated ClientList) from the introducer
    buffer := make([]byte, 1024)
    n, err := conn.Read(buffer)
    if err != nil {
        fmt.Println("Error receiving updated ClientList from introducer:", err)
        return
    }

	connectTime := time.Now()

    var tmpClientList []ClientInfo
    if err := json.Unmarshal(buffer[:n], &tmpClientList); err != nil {
        fmt.Println("Error decoding updated ClientList from JSON:", err)
        return
    }

    fmt.Println("Successfully connected to introducer")
    mergeClientList(tmpClientList, connectTime)
}

//Prints the current client list
func printClientList(){
	if !isVerbose {
		return
	}

	fmt.Println("==================================")
	
	for _, client := range ClientList {
		fmt.Println(client.Hostname,client.HeartBeatCount,client.TimeStamp)
	}
	fmt.Println("==================================")

}

//initalize logger and clears old logs
func loggerInit() {
	var err error
	hostnameOutput, err := os.Hostname()

	if err != nil {
		fmt.Println("Error when fetching host name!")
		return
	}

	hostname := string(hostnameOutput)

	// Extract vm number from host name (fa23-cs425-32##.cs.illinois.edu)
	index := strings.Index(hostname, ".")

	// If '.' doesnt exist or is in the first two characters
	if index < 2 {
		fmt.Println("Unable to find machine number from host name!")
		return 
	}

	num, err := strconv.Atoi(hostname[index-2 : index])
	if err != nil {
		fmt.Printf("Error converting string to int: %s\n", err)
	}
	logFilename := fmt.Sprintf("machine.%d.log", num)
	logFile, err = os.OpenFile(logFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening file for logging: %v", err)
	}

	logger = log.New(logFile, "customLogPrefix: ", log.LstdFlags)
}

func loggerCleanup() {
	if logFile != nil {
		logFile.Close()
	}
}

//Helper function for sender, increase heatbeat every gossip round
func incrementHeatBeat(){
	HostName, err := os.Hostname()
	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}
	clientListMutex.Lock()
    defer clientListMutex.Unlock()
	for i := range ClientList {
		// No need to update / cleanup self
		if HostName == ClientList[i].Hostname {
			ClientList[i].HeartBeatCount += 1
			break
		}
	}
	return
}

//Gossips the local clientlist to a peer
func gossip(peerHostname string){
  
	if peerHostname == "" {
		return
	}
 


    message := Message {
        Command: CommandMode,
        ClientList: ClientList,
    }
    

    
    jsonData, err := json.Marshal(message)
    if err != nil {
        fmt.Println("Error encoding message to JSON:", err)
        return
    }

	byteUsage += len(jsonData)

    
    peerPort := UDPPort
    addr, err := net.ResolveUDPAddr("udp", peerHostname + peerPort)
    if err != nil {
        fmt.Println("Error resolving address:", err)
        return
    }
    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error dialing UDP:", err)
        return
    }
    defer conn.Close()
    _, err = conn.Write(jsonData)
    if err != nil {
        fmt.Println("Error sending data:", err)
        return
    }
	if isVerbose {
    	fmt.Println("Gossiping message")
	}
	printClientList()
}

//Shuffles local client list, gossips to three member, and repeat
func sender(){
	selfHostName, _ := os.Hostname()
	
	for true{
		if isPaused{
			continue
		}
        hostnames := make([]string, len(ClientList))

		if isVerbose {
			fmt.Println("Gossiper took lock at: ", time.Now())
		}
        clientListMutex.Lock() // Lock before reading ClientList
        for i, client := range ClientList {
			// ignore self
			if selfHostName != client.Hostname {
				hostnames[i] = client.Hostname
			}
        }
        clientListMutex.Unlock()
		if isVerbose {
			fmt.Println("Gossiper released lock at: ", time.Now())
		}


        // Shuffle the hostnames
        rand.Shuffle(len(hostnames), func(i, j int) {
            hostnames[i], hostnames[j] = hostnames[j], hostnames[i]
        })

		transmitMutex.Lock()
        for i := 0; i < len(hostnames); i += 3 {
            end := i + 3
            if end > len(hostnames) {
                end = len(hostnames)
            }

			incrementHeatBeat()
			

            // Process the current set of clients using shuffled indices

            for _, peerHostname := range hostnames[i:end] {
                gossip(peerHostname)
            }

            // Sleep for 500ms
			time.Sleep(500 * time.Millisecond)
        }
        // After gossiping a gossip / gossip+s, reset command to do nothing to avoid repeat
		transmitMutex.Unlock()
        CommandMode = NOTHING
		time.Sleep(100 * time.Millisecond)
    }

	
}

//Constanly checking for change in client status: i.e. fails, sus, cleanups
func checkTimeout(){
	HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}

	for {
		if isPaused{
			continue
		}
		// Wait for 0.5 second before checking each client's timestamp
		time.Sleep(100 * time.Millisecond)

		clientListMutex.Lock()
		
		if isVerbose { fmt.Println("acquired lock",time.Now()) }
		// Create a new list for updated clients
		newClientList := make([]ClientInfo, 0)

		// Note: Delete client by not appending to the new list
		for _, client := range ClientList {
			// No need to update / cleanup self
			if HostName == client.Hostname {
				newClientList = append(newClientList, client)
				continue
			}

			elapsedTime := time.Since(client.TimeStamp)

			// Should clean up node if its marked as failed and TCleanup has passed
			if client.SFlag == FAIL && elapsedTime > TCleanup {
				logger.Printf("Failed Node %s cleaned up at time %s ", client.Hostname, time.Now())
				continue
			}

			// If Norm or Susp, check for timeout to mark as suspicious or failed
			if client.SFlag == NORM || client.SFlag == SUSP {				
				if IsSusEnabled {
					// Gossip + S
					if client.SFlag == NORM && elapsedTime > TSus {
						client.SFlag = 1
						client.TimeStamp = time.Now()
						fmt.Printf("Node %s marked as suspicious at time %s \n", client.Hostname, time.Now())
					} else if client.SFlag == SUSP && elapsedTime > TFailWithSus {
						client.SFlag = 2
						client.TimeStamp = time.Now()
						falsePositiveCount += 1
						logger.Printf("Node %s marked as failed at time %s ", client.Hostname, time.Now())
					}
				} else {
					// Gossip
					if client.SFlag == NORM && elapsedTime > Tfail {
						client.SFlag = 2
						client.TimeStamp = time.Now()
						falsePositiveCount += 1
						logger.Printf("Node %s marked as failed at time %s ", client.Hostname, time.Now())
					}
				}
			} 

			// Add to new list if not cleaned up
			newClientList = append(newClientList, client)
		}

		ClientList = newClientList

		clientListMutex.Unlock()
		if isVerbose { fmt.Println("Released lock",time.Now()) }
	}
}


//Listener runs on UDP and receives and updates clientlists according to gossips
func listener(){
	HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}

	udpAddr, err := net.ResolveUDPAddr(Network, HostName+UDPPort)
	udpConn, err := net.ListenUDP("udp", udpAddr)
    if err != nil {
        fmt.Println("Error creating UDP connection:", err)
        return
    }
    defer udpConn.Close()
	fmt.Println("UDP listening on", HostName)
	buffer := make([]byte, 1024*10)

	var msgResponse Message

	for {
		if isPaused{
			// Messages that arrive during this time will wait and will be read when this resumes, leading to ghost messages
			// so we need to consume and ignore them while paused
			udpConn.ReadFromUDP(buffer)
			continue
		}
		n, _, err := udpConn.ReadFromUDP(buffer)
		if err != nil{
			fmt.Println("Error reading from udp:", err)
		}

		// Artificial message drop rate
		// TODO: move to command line input
		messageDropRate := 0.0
		randValue := rand.Float64()

		if randValue < messageDropRate {
			// Artifically drop message
			// fmt.Println("Message Dropped")
			continue;
		} 


		byteUsage += n

		gossipTime := time.Now()

		// Deserialize the received data into ClientList
		if err := json.Unmarshal(buffer[:n], &msgResponse); err != nil {
			fmt.Println("Error decoding updated Message from JSON:", err)
			continue
		}

		if isVerbose {
			// Update local client list
			fmt.Println("Gossip received ========")
			fmt.Println(msgResponse.ClientList)
			fmt.Println(gossipTime)
		}


		mergeClientList(msgResponse.ClientList, gossipTime)

	
		// Update gossip mode 
		if (msgResponse.Command != 0) && !((msgResponse.Command == GOSSIP_SUS && IsSusEnabled) || (msgResponse.Command == GOSSIP && !IsSusEnabled))  {
			IsSusEnabled = msgResponse.Command == GOSSIP_SUS

			// TODO: remove
			if (IsSusEnabled) {
				logger.Printf("Switching to gossip+s at %s", time.Now())
				fmt.Println("Switch to Gossip + sus Only")
				
			} else {
				logger.Printf("Switching to gossip at %s", time.Now())
				fmt.Println("Switch to Gossip Only")

			}

			CommandMode = msgResponse.Command
		}
	}
}


// Might overflow
func printBandwidthPerSec() {
	totalUsage := 0
	iteration := 0
	for true {
		time.Sleep(time.Second)
		totalUsage += byteUsage
		iteration += 1
		byteUsage = 0

		fmt.Printf("Average bandwidth: %d (bytes/sec)\n", (totalUsage / iteration))
	}
}

// Assuming you dont exit any node
func printFalsePositivePerMin() {
	for true {
		time.Sleep(time.Minute)
		fmt.Printf("False postive rate: %d (count/minute)\n", (	falsePositiveCount ))
		falsePositiveCount = 0
	}
}



/// =================================================================/// 
/// MP3 
func extractID(hostname string) string{
	re := regexp.MustCompile(`fa23-cs425-32(\w\w)\.cs\.illinois\.edu`)
	matches := re.FindStringSubmatch(hostname)
	return matches[1]
}

func getHostNameFromId(id string) string {
	return fmt.Sprintf("fa23-cs425-32%s.cs.illinois.edu", id)
}

func getHostNamesFromIds(ids []string) []string {
	var hostnames []string
	for _, id := range ids {
		hostnames = append(hostnames, getHostNameFromId(id))
	}
	return hostnames
}

func isInList(list []string, id string) bool {
	for _, value := range list {
		if value == id {
			return true
		}
	}
	return false
}

func getLocalFileNames() []string{
	files, err := ioutil.ReadDir(FileFolder)
	if err != nil {
		log.Fatalf("Failed reading directory: %s", err)
	}

	var filenames []string
	for _, file := range files {
		if !file.IsDir() { // Check if it's a file and not a directory
			filenames = append(filenames, file.Name())
			
		}
	}

	return filenames
}

func getOwnedFiles(id string, filenames []string) []string{
	var ownedFiles []string
	for _, filename := range filenames {
		fileKey := hash(filename)

		owner := getOwner(fileKey)

		if owner == "" || owner != id {
			continue
		}

		ownedFiles = append(ownedFiles, filename)
	}

	return ownedFiles
}

func getFilesToDelete(id string) []string{
	var toDelete []string

	localFiles := getLocalFileNames()
	
	for _, filename := range localFiles {
		fileKey := hash(filename)
		ownerId := getOwner(fileKey)
		successorsIds := getSuccessors(hash(ownerId))

		// If not a owner or successor of the owner of the file we can delete it
		if (ownerId == "" || ownerId != id) && !isInList(successorsIds, id) {
			toDelete = append(toDelete, filename)
		}
	}
	return toDelete
}

func getAddedFiles(prevFilenames [] string, afterFilenames []string) []string {
	beforeMap := make(map[string]bool)
	for _, s := range prevFilenames {
		beforeMap[s] = true
	}

	var added []string
	for _, s := range afterFilenames {
		if !beforeMap[s] {
			added = append(added, s)
		}
	}

	return added
}

func getRemovedFiles(prevFilenames [] string, afterFilenames []string) []string {
	afterMap := make(map[string]bool)
	for _, s := range afterFilenames {
		afterMap[s] = true
	}

	var removed []string
	for _, s := range prevFilenames {
		if !afterMap[s] {
			removed = append(removed, s)
		}
	}

	return removed
}

// Check for client leave/join and redistribute files
func monitorClientList() {
	var prevClientList []ClientInfo
	waitDuration := 1 * time.Second // Statically defined wait duration

	HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}

	selfID := extractID(HostName)
	selfKey := hash(selfID)

	for {
		if !isPaused {
			currentList := make([]ClientInfo, len(ClientList))
			clientListMutex.Lock()
			copy(currentList, ClientList)
			clientListMutex.Unlock()

			if !clientListsEqual(prevClientList, currentList) {
				// Wait for a bit in case it was a false positive fail and the node rejoined immediately
				// Note: Not sure if this works, but hasnt seen random fail
				time.Sleep(waitDuration) 

				clientListMutex.Lock()
				afterWaitList := make([]ClientInfo, len(ClientList))
				copy(afterWaitList, ClientList)
				clientListMutex.Unlock()

				// Check again if the client list is still different from previous
				if !clientListsEqual(prevClientList, afterWaitList) {
					for _, client := range prevClientList {
						if !contains(afterWaitList, client) {
							fmt.Printf("Client with Hostname: %s has left.\n", client.Hostname)
							id := extractID(client.Hostname)

							prevSuccessors := getSuccessors(selfKey)
							isLeaverInPrevSuccessors := isInList(prevSuccessors, id)

							prevPredecessor := getPredecessor(selfKey)
							isLeaverPrevPredecessor := prevPredecessor == id

							prevOwnedFiles := getOwnedFiles(selfID, getLocalFileNames())
							// fmt.Println("Previously owned files: ", prevOwnedFiles)

							removeNode(id)
							printDHT()

							ownedFiles := getOwnedFiles(selfID, getLocalFileNames())
							// fmt.Println("Owned files: ", ownedFiles)

							if isLeaverInPrevSuccessors {
								// If the node that left is one of the replicas, send to rereplicate to next logical node
								fmt.Printf("Client with Hostname: %s which left was a successor\n", client.Hostname)

								successors := getSuccessors(selfKey)

								if len(successors) == 0 || isInList(prevSuccessors, successors[len(successors)-1]){
									fmt.Println("No new Successor to replicate to.")
									continue
								}

								nextSuccessorID := successors[len(successors)-1]
								nextSuccessorHostName := getHostNameFromId(nextSuccessorID)

								// Send files to replicate
								for _, filename := range ownedFiles {
									fmt.Printf("Sending %s to new Successor with Hostname: %s\n", filename, nextSuccessorHostName)

									putFile(filename,nextSuccessorHostName, true, false)
								}

							} 
							
							if isLeaverPrevPredecessor {
								// If predecessor leaves and this node becomes to main storage for a file, send to replica if needed
								fmt.Printf("Client with Hostname: %s which left was the predecessor\n", client.Hostname)
								
								newFilesWeOwn := getAddedFiles(prevOwnedFiles, ownedFiles)
								successors := getSuccessors(selfKey)

								if len(successors) == 0 {
									continue
								}

								// We lazily replicate to all successors so we don't have to check which successors have the file in the case that all 3 predecessor leaves
								fmt.Println("Replicating these files: ", newFilesWeOwn, " to successors: ", successors)

								for _, successor := range successors {
									successorHostName := getHostNameFromId(successor)

									// Send files to replicate
									for _, filename := range newFilesWeOwn {
										putFile(filename,successorHostName, true, false)
									}
								}
							}
						}
					}
					for _, client := range afterWaitList {
						if !contains(prevClientList, client) && client.SFlag != FAIL {
							fmt.Printf("Client with Hostname: %s has joined.\n", client.Hostname)

							id := extractID(client.Hostname)

							prevOwnedFiles := getOwnedFiles(selfID, getLocalFileNames())
							// fmt.Println("Prev owned files: ", prevOwnedFiles)

							addNode(id)
							printDHT()

							if client.Hostname == HostName {
								continue
							}
	
							successors := getSuccessors(selfKey)
							// fmt.Println("Successors: ", successors)
							isJoinerInSuccessors := isInList(successors, id)

							// connect to new node's rpc server
							err := connectToServer(client.Hostname)
							if err != nil {
								fmt.Printf("Error connecting to client %s 's rpc server.\n", client.Hostname)
							}

							predecessor := getPredecessor(selfKey)
							// fmt.Println("Predecessor: ", predecessor)
							isJoinerPredecessor := predecessor == id

							ownedFiles := getOwnedFiles(selfID, getLocalFileNames())
							// fmt.Println("Currently Owned files: ", ownedFiles)

							if isJoinerPredecessor {
								// If joiner is the predecessor, need to send file for it to own
								fileNewNodeOwns := getRemovedFiles(prevOwnedFiles, ownedFiles)

								for _, filename := range fileNewNodeOwns {
									fmt.Printf("Sending %s to New owner with Hostname: %s\n", filename, client.Hostname)
									putFile(filename, client.Hostname, true, false)
								}

							} 
							
							if isJoinerInSuccessors {
								// If joiner is in successors, need to send file to replicate

								for _, filename := range ownedFiles {
									fmt.Printf("Sending %s to Successor with Hostname: %s\n", filename, client.Hostname)
									putFile(filename, client.Hostname, true, false)
								}

							} 

							// Delete unneeded duplicates
							// Scan local files to see if we own or is successor of, if not, delete
							filesToDelete := getFilesToDelete(selfID)
							if len(filesToDelete) != 0 {
								fmt.Println("Removing duplicated files: ", filesToDelete)
							}

							for _, filename := range filesToDelete {
								// just using the grpc delete since its safe
								deleteFile(filename, HostName)
							}

						}
					}
					prevClientList = afterWaitList
				}
			}

		} else {
			prevClientList = []ClientInfo{}
		}
		time.Sleep(waitDuration)
	}
}

// TODO
// This func should take in GET, PUT, DELETE and a filename
// Determines what nodes to call based on chord ring and quorum
// Calls the sender function for commands
func handleSendingFileCommand(input string) {
	//HostName, err := os.Hostname()
	parts := strings.SplitN(input, " ", 2)
	cmd := parts[0]
	filename := parts[1]

	filename = strings.TrimSuffix(filename, "\n")
	currentTime := time.Now()
    fmt.Printf("CMD time is: %s\n", currentTime)

	switch cmd{
	case "get":
		ownerId := getOwner(hash(filename))
		owner := getHostNameFromId(ownerId)
		successorsIds := getSuccessors(hash(ownerId))
		successors := getHostNamesFromIds(successorsIds)

		fmt.Println("\n========= Printing Command =========")
		fmt.Printf("Reading file %s from the following:\n", filename)
		fmt.Println("Owner:", owner)
		fmt.Println("Replicas:", successors)
		fmt.Println("=========== End of Print ===========\n")

		assignedNodes := append(successors, owner)

		timestamp, err := getFile(filename, assignedNodes)
		if err != nil {
			fmt.Println("Error getting file:", err)
			return
		}
		timestampMap[filename] = timestamp
		fmt.Println("File timestamp:", timestamp)
		currentTime := time.Now()
    	fmt.Printf("received time: %s\n", currentTime)
	case "put":
		//figure out who to send it to

		ownerId := getOwner(hash(filename))
		owner := getHostNameFromId(ownerId)
		successorsIds := getSuccessors(hash(ownerId))
		successors := getHostNamesFromIds(successorsIds)

		fmt.Println("\n========= Printing Command =========")
		fmt.Printf("Writing file %s to the following:\n", filename)
		fmt.Println("Owner:", owner)
		fmt.Println("Replicas:", successors)
		fmt.Println("=========== End of Print ===========\n")

		assignedNodes := append(successors, owner)
		
		for _, targetHostname := range assignedNodes {
			putFile(filename, targetHostname, false, false)
			// if err != nil {
			// 	fmt.Printf("Error sending file to %s: %v\n", targetHostname, err)
			// }
		}
		currentTime := time.Now()
    	fmt.Printf("received time: %s\n", currentTime)
	case "append":
		// Determine the target nodes for appending the file
        ownerId := getOwner(hash(filename))
        owner := getHostNameFromId(ownerId)
        successorsIds := getSuccessors(hash(ownerId))
        successors := getHostNamesFromIds(successorsIds)

        fmt.Println("\n========= Printing Command =========")
        fmt.Printf("Appending to file %s at the following:\n", filename)
        fmt.Println("Owner:", owner)
        fmt.Println("Replicas:", successors)
        fmt.Println("=========== End of Print ===========\n")

        assignedNodes := append(successors, owner)

        // Append the file content to each assigned node
        for _, targetHostname := range assignedNodes {
            err := putFile(filename, targetHostname, false, true)
            if err != nil {
                fmt.Printf("Error appending file %s to %s: %v\n", filename, targetHostname, err)
            }
        }
        fmt.Printf("Append operation completed for %s\n", filename)
    

	case "delete":
		//figure out who to send it to
		ownerId := getOwner(hash(filename))
		owner := getHostNameFromId(ownerId)
		successorsIds := getSuccessors(hash(ownerId))
		successors := getHostNamesFromIds(successorsIds)

		fmt.Println("\n========= Printing Command =========")
		fmt.Printf("Deleting file %s from the following:\n", filename)
		fmt.Println("Owner:", owner)
		fmt.Println("Replicas:", successors)
		fmt.Println("=========== End of Print ===========\n")

		assignedNodes := append(successors, owner)

		for _, targetHostname := range assignedNodes {
			err := deleteFile(filename, targetHostname)
			if err != nil {
				fmt.Printf("Error seleting file %s from %s: %v\n", filename, targetHostname, err)
			}
		}
		currentTime := time.Now()

    	fmt.Printf("received time: %s\n", currentTime)
	case "ls":
		// What if the file doesnt exist

		ownerId := getOwner(hash(filename))
		owner := getHostNameFromId(ownerId)
		successorsIds := getSuccessors(hash(ownerId))
		successors := getHostNamesFromIds(successorsIds)

		fmt.Println("\n========= Printing Command =========")
		fmt.Printf("file %s is being stored at the following:\n", filename)
		fmt.Println("Owner:", owner)
		fmt.Println("Replicas:", successors)
		fmt.Println("=========== End of Print ===========\n")
	}
}

func startMultiRead(input string) {
	parts := strings.Fields(input)
	if len(parts) < 3 {
		fmt.Println("Invalid input. Format: multiread <filename> <hostname1> <hostname2> ...")
		return
	}

	filename := parts[1]
	hostnameIds := parts[2:]

	for _, hostnameId := range hostnameIds {
		go sendMultiRead(getHostNameFromId(hostnameId), filename)
	}
}

func sendMultiRead(hostname string, filename string) {
	conn, err := net.Dial("tcp4", hostname+MultireadPort)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(filename + "\n"))
	if err != nil {
		fmt.Println("Error sending multiread filename to", hostname, ":", err.Error())
	}
	fmt.Println("Multiread for ", filename, " sent to", hostname)
}

func multiReadListener() {
	HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", HostName+MultireadPort)
	tcpListener, err := net.ListenTCP("tcp4", tcpAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer tcpListener.Close()

	for {
		conn, err := tcpListener.Accept()

		if !isPaused {
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				continue
			}
			go handleMultiRead(conn)
		} 
	}
}

func handleMultiRead(conn net.Conn) {
	defer conn.Close()

	buffer, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}

	filename := strings.TrimSpace(string(buffer))
	fmt.Println("Received multiread filename:", filename)

	command := "get " + filename

	handleSendingFileCommand(command)
}

/// =================================================================/// 

//The userthread handles all the user input responses
func userThread(){
	reader := bufio.NewReader(os.Stdin)
	for true{
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		if strings.Contains(input, "exit"){
			if isPaused{
				fmt.Println("Current Node already paused")
			}else{
				isPaused = true
				fmt.Println("Exited self at: ", time.Now())
			}

		}else if strings.Contains(input, "list_mem"){
			fmt.Println("Current Membership Status")
			fmt.Println("==================================")
	
			for _, client := range ClientList {
				fmt.Println(client.Hostname,client.HeartBeatCount,client.TimeStamp ,client.SFlag)
			}
			fmt.Println("==================================")

		}else if strings.Contains(input, "list_self"){
			hostname, _ :=os.Hostname()
			fmt.Printf("My Hostname: %s, Port: %s \n",hostname,UDPPort)

		}else if strings.Contains(input, "gossip+sus"){
			if IsSusEnabled{
				fmt.Println("Suspicion already enabled")
			}else{
				transmitMutex.Lock()
				IsSusEnabled = true
				CommandMode = GOSSIP_SUS

				// todo: remove
				logger.Printf("Switching to gossip+s at %s", time.Now())
				transmitMutex.Unlock()
			}
		

		}else if strings.Contains(input, "gossip"){
			if !IsSusEnabled{
				fmt.Println("Suspicion already disabled")
			}else{
				transmitMutex.Lock()
				IsSusEnabled = false
				CommandMode = GOSSIP

				// todo: remove
				logger.Printf("Switching to gossip at %s", time.Now())
				transmitMutex.Unlock()
			}
			
		}else if input == "join\n" {
			if !isPaused{
				fmt.Println("Current Node already joined")
			}else{
				ClientList = ClientList[:0]
				HostName, _ := os.Hostname()
				clientInfo := ClientInfo{
					Hostname: HostName,
					Port:     UDPPort,
					HeartBeatCount: 1,
					TimeStamp: time.Now(),
					SFlag: NORM,
				}
				ClientList = append(ClientList, clientInfo)
				isPaused = false
				pingIntroducer()

			}

		}else if strings.Contains(input, "get"){

			handleSendingFileCommand(input)
		}else if strings.Contains(input, "delete"){
			handleSendingFileCommand(input)

		}else if strings.Contains(input, "put"){
			handleSendingFileCommand(input)

		}else if strings.Contains(input, "ls"){
			handleSendingFileCommand(input)

		}else if strings.Contains(input, "store"){
			localFiles := getLocalFileNames()
			fmt.Println("The files stored locally are:")
			fmt.Println(localFiles)

		}else if strings.Contains(input, "multiread"){
			startMultiRead(input)
		}else if strings.Contains(input, "maple"){
			cmdPacket, _ := parseInput(input)
			callLeader(cmdPacket)
		}else if strings.Contains(input, "juice"){
			cmdPacket, _ := parseInput(input)
			callLeader(cmdPacket)
		}else if strings.Contains(input, "sql_filter"){
			cmdPacket, _ := parseInput(input)
			callLeader(cmdPacket)
		}else if strings.Contains(input, "sql_join"){
			cmdPacket, _ := parseInput(input)
			callLeader(cmdPacket)
		}else if strings.Contains(input, "traffic"){
			cmdPacket, _ := parseInput(input)
			callLeader(cmdPacket)
		}else{
			fmt.Println("Invalid Input, please choose the following:")
			fmt.Println("exit // join // gossip // gossip+sus // list_mem // list_self")
			fmt.Println(" put // get // delete // put // ls // store // multiread")

		}

	}

}

// start introducer with go run main.go -intro
// Use -verbose to print debug
// Use -bps to print bandwidth per sec (bytes)

func main() {
	var isIntro = flag.Bool("intro", false, "")
	var verboseFlag = flag.Bool("verbose", false, "")
	var bandwidthPerSecFlag = flag.Bool("bps", false, "")
	var falsePositiveFlag = flag.Bool("fp", false, "")
	var isLeader = flag.Bool("leader", false, "")



	flag.Parse()
	isVerbose = *verboseFlag
	isPaused = false


	rand.Seed(time.Now().UnixNano())

	loggerInit()
	defer loggerCleanup()

	HostName, err := os.Hostname()

	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return
	}
	clientInfo := ClientInfo{
		Hostname: HostName,
		Port:     UDPPort,
		HeartBeatCount: 1,
		TimeStamp: time.Now(),
		SFlag: NORM,
	}
	ClientList = append(ClientList, clientInfo)
	

	if *isIntro {
		go introducer()
	}else{
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("\nEnter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.Contains(input, "join") {
			pingIntroducer()
			
		}
	}

	if *isLeader{
		go startLeader()
	}
	go startMapleServer()
	go startJuiceServer()


	go sender()
	go listener()
	go checkTimeout()
	go monitorClientList()
	go startServer()
	go putListener()
	go appendListener()
	go multiReadListener()

	err = connectToServer(HostName)
	if err != nil {
		fmt.Println("Error connecting to it's own server:", err)
	}

	if (*bandwidthPerSecFlag) {
		go printBandwidthPerSec()
	}
	if (*falsePositiveFlag) {
		go printFalsePositivePerMin()
	}

	userThread()

}



