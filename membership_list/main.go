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



const (
	Network = "udp"
	IntroPort  = ":4444"
	UDPPort = ":8888"
	IntroHostname = "fa23-cs425-3207.cs.illinois.edu"
	NORM = 0
	SUSP = 1
	FAIL = 2
	Tfail = 4 * time.Second
	TFailWithSus = 3500 * time.Millisecond
	TSus = 3000 * time.Millisecond
	TCleanup = 5000 * time.Millisecond
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
    return false // No duplicate found
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
 
    // TODO (Find a better solution): Should gossip the Message struct which will carry the client list and an additional flag for gossip / gossip+s (CommandMode)


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
			
		}else if strings.Contains(input, "join"){
			if !isPaused{
				fmt.Println("Current Node already joined")
			}else{
				isPaused = false
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
				pingIntroducer()
				
			}

		}else{
			fmt.Println("Invalid Input, please choose the following:")
			fmt.Println("exit // join // gossip // gossip+sus // list_mem // list_self")
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


// start introducer with go run main.go -intro
// Use -verbose to print debug
// Use -bps to print bandwidth per sec (bytes)

//To-do: change join logic/ update client timestamp/ rm old list
func main() {
	var isIntro = flag.Bool("intro", false, "")
	var verboseFlag = flag.Bool("verbose", false, "")
	var bandwidthPerSecFlag = flag.Bool("bps", false, "")
	var falsePositiveFlag = flag.Bool("fp", false, "")
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
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.Contains(input, "join") {
			pingIntroducer()
			
		}

	}

	go sender()
	go listener()
	go checkTimeout()

	if (*bandwidthPerSecFlag) {
		go printBandwidthPerSec()
	}
	if (*falsePositiveFlag) {
		go printFalsePositivePerMin()
	}

	userThread()

}



