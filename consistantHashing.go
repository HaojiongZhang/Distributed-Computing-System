package main

import (
	"fmt"
	"crypto/sha256"
	"sync"
	"sort"
)

const (
	MOD = 1024
	MOD_MASK = MOD - 1
)
var (
	DHT = make(map[uint32]string)
	
	DHTMutex sync.Mutex
)

func hash(input string) uint32 {
	hashValue := sha256.Sum256([]byte(input))
	var numericHash uint32
	for i := 0; i < len(hashValue); i += 4 {
		numericHash += uint32(hashValue[i])<<24 | uint32(hashValue[i+1])<<16 | uint32(hashValue[i+2])<<8 | uint32(hashValue[i+3])
	}
	
	finalHash := numericHash & MOD_MASK
	return finalHash
}

func addNode(hostname string){
	hashVal := hash(hostname)
	DHTMutex.Lock()
	defer DHTMutex.Unlock()
	for k,_ := range DHT {
		if k == hashVal{
			hashVal = k+1
		}
	}

	DHT[hashVal] = hostname
}

func removeNode(hostname string) {
	hashVal := hash(hostname)
	DHTMutex.Lock()
	defer DHTMutex.Unlock()
	if _, exists := DHT[hashVal]; exists {
		delete(DHT, hashVal)
	} else {
		fmt.Println("Node not found in DHT!")
	}
}

func getDHT() map[uint32]string {
	DHTMutex.Lock()
	tempDHT := make(map[uint32]string, len(DHT))
	for k, v := range DHT {
		tempDHT[k] = v
	}
	DHTMutex.Unlock()
	return tempDHT
}


func getSuccessors(key uint32) []string {
	// Copy the map while holding the lock
	DHTMutex.Lock()
	tempDHT := make(map[uint32]string, len(DHT))
	for k, v := range DHT {
		tempDHT[k] = v
	}
	DHTMutex.Unlock()

	// Collect all keys from the copied map
	keys := make([]uint32, 0, len(tempDHT))
	for k := range tempDHT {
		keys = append(keys, k)
	}

	// Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the position of the key and get its 3 successors
	result := []string{}
	for i, k := range keys {
		if k > key {
			for j := 0; j < 3; j++ {
				index := (i+j)%len(keys)
				
				if keys[index] == key {
					return result // Return if we encounter the original key while wrapping around
				}
				result = append(result, tempDHT[keys[index]])
			}
			return result
		}
	}

	// If key is not found or the successors wrap around
	for j := 0; j < 3 && j < len(keys); j++ {
		if keys[j] == key {
			return result // Return if we encounter the original key while wrapping around
		} 
		
		if len(result) < 3 {
			result = append(result, tempDHT[keys[j]])
		} 
	}

	return result
}

func getPredecessor(key uint32) string {
	// Copy the map while holding the lock
	DHTMutex.Lock()
	tempDHT := make(map[uint32]string, len(DHT))
	for k, v := range DHT {
		tempDHT[k] = v
	}
	DHTMutex.Unlock()

	// Collect all keys from the copied map
	keys := make([]uint32, 0, len(tempDHT))
	for k := range tempDHT {
		keys = append(keys, k)
	}

	if len(keys) < 2 {
		return ""
	}

	// Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the predecessor of the key
	for i, k := range keys {
		if k == key {
			if i == 0 { // If the key is the smallest in the map
				return tempDHT[keys[len(keys)-1]]
			}
			return tempDHT[keys[i-1]]
		}
	}

	return ""
}

func getOwner(inputKey uint32) string {
	// Copy the map while holding the lock
	DHTMutex.Lock()
	tempDHT := make(map[uint32]string, len(DHT))
	for k, v := range DHT {
		tempDHT[k] = v
	}
	DHTMutex.Unlock()

	// Collect all keys from the copied map
	keys := make([]uint32, 0, len(tempDHT))
	for k := range tempDHT {
		keys = append(keys, k)
	}

	// If the map is empty
	if len(keys) == 0 {
		return ""
	}

	// Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the next greatest key
	for _, k := range keys {
		if k >= inputKey {
			return tempDHT[k]
		}
	}

	// If no greater key found, wrap and return the smallest key
	return tempDHT[keys[0]]
}

func isInSuccessors(selfKey uint32, id string) bool {
	successors := getSuccessors(selfKey)

	for _, value := range successors {
		if value == id {
			return true
		}
	}
	return false
}


func printDHT(){
	fmt.Println("\n======== Printing DHT ========")
	for k, v := range DHT {
		fmt.Printf("Key: %d, Value: %s\n", k, v)
	}
	fmt.Println("======== End of Print ========\n")
}

func assignFileToNode(file string, count int) []string {
	fileHash := hash(file)
	fmt.Println("File hash:", fileHash)
	DHTMutex.Lock()
	defer DHTMutex.Unlock()

	// Check if DHT has at least four nodes
	if len(DHT) < count {
		fmt.Println("Warning: DHT has fewer than %s nodes!", count)
		var availableNodes []string
		for _, nodeNumber := range DHT {
			formattedNode := fmt.Sprintf("fa23-cs425-32%s.cs.illinois.edu", nodeNumber)
			availableNodes = append(availableNodes, formattedNode)
		}
		return availableNodes
	}

	// 2. Sort the keys of the DHT map
	var sortedKeys []uint32
	for k := range DHT {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Slice(sortedKeys, func(i, j int) bool { return sortedKeys[i] < sortedKeys[j] })

	fmt.Print(sortedKeys)
	// 3. Find the index of the closest node that hashes right behind the filename's hash
	var closestNodeIndex int = len(sortedKeys) - 1
	for i, key := range sortedKeys {
		if key >= fileHash {
			closestNodeIndex = i 
			break
		}
	}

	// If all nodes hash before the file hash, wrap around to the last node
	if closestNodeIndex < 0 {
		closestNodeIndex = len(sortedKeys) - 1
	}

	// 4. Get the next four nodes starting from the closestNodeIndex
	var nextNodes []string
	for i := 0; i < count; i++ {
		nodeNumber := DHT[sortedKeys[closestNodeIndex]]
		formattedNode := fmt.Sprintf("fa23-cs425-32%s.cs.illinois.edu", nodeNumber)
		nextNodes = append(nextNodes, formattedNode)
		closestNodeIndex = (closestNodeIndex + 1) % len(sortedKeys)
	}
	return nextNodes
}




// func main() {

// 	file := "example.txt"
// 	addNode("01")
// 	addNode("02")

// 	addNode("03")
// 	addNode("04")
// 	addNode("05")
// 	addNode("06")
// 	addNode("07")
// 	addNode("08")
// 	addNode("09")
// 	addNode("10")
// 	addNode("2")

// 	printDHT()
	
// 	removeNode("2")
// 	removeNode("10")

// 	printDHT()
// 	assignedNodes := assignFileToNode(file,1)
// 	fmt.Printf("File '%s' should be stored on hostnames: %v\n", file, assignedNodes)

// }
