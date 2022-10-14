package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"net"
	"time"
	"sync"
//	"sort"
)


type entry []byte
type registry []entry

func (r registry) Len() int {
	return len(r)
}

func (r registry) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r registry) Less(i, j int) bool {
	for b := 0; b < 10; b++ {
		if r[i][b] > r[j][b] {
			return false
		} else if r[i][b] < r[j][b] {
			return true
		}
	}
	return false

}
type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func listenForData(ch chan<- []byte, serverId int, cType string, host string, port string) {
	fmt.Println("Server " + strconv.Itoa(serverId) + " Starting " + cType + " server on connHost: " + host + ", connPort: " + port)
	l, err := net.Listen(cType, host+":"+port)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
			return
		}
		fmt.Println("Server " + strconv.Itoa(serverId) + " Client " + conn.RemoteAddr().String() + " connected in listener.")
		go handleConnection(conn, ch)

	}
}

func handleConnection(conn net.Conn, ch chan<- []byte) {
	buff := make([]byte, 101)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Conn::Read: err %v\n", err)
		os.Exit(1)
	}
	ch <- buff
}

func consolidateServerData(ch <-chan []byte, data []byte, serverId int, numOfClients int) {
	numOfClientsCompleted := 0
	for {
		if numOfClientsCompleted == numOfClients {
			break
		}
		message := <-ch // receive data from channel
		if(message[0] == byte(0xFF)) {
			numOfClientsCompleted++
			fmt.Println("Server ", serverId, " Found an end of stream msg")
		}
		data = append(data, message[1:101]...)
		fmt.Println("Server " + strconv.Itoa(serverId) + "Got message starting with " + strconv.Itoa(int(message[1])) + " completed " + strconv.Itoa(numOfClientsCompleted) + " clients")
	}
}

func bitsToShift(serverNum int) int {
	if(serverNum > 8) {
		return 4
	} else if(serverNum > 4) {
		return 3
	} else if(serverNum > 2) {
		return 2
	}
	return 1

}

func sendData(data []byte, cType string, serverNum int, senderId int, receiverId int, host string, port string) {
	fmt.Printf("Server %d requesting %s connection on connHost: %s:%s\n", senderId, cType, host, port)
	d := net.Dialer{Timeout: 100*time.Millisecond}
	conn, err := d.Dial("tcp", host+":"+port)
	for err != nil {
		time.Sleep(500*time.Millisecond)
		fmt.Printf("Server %d retrying %s connection on connHost: %s:%s\n", senderId, cType, host, port)
		conn, err = net.Dial(cType, host+":"+port)
	}
	if err != nil  {
		log.Fatalf("Server %d failed to connect to %s:%s: %s", senderId, cType, host, port, err)
	}
	fmt.Println("Server " + strconv.Itoa(senderId) + " connected to " + host + ":" + port)

	shift := byte(bitsToShift(serverNum))
	for i := 0; i < len(data); i = i+100 {
		if int(data[i] >> shift) == receiverId {
			dataToSend := append([]byte{0xFF}, data[i:i+100]...)
			conn.Write(dataToSend)
			fmt.Printf("Server %d sent message starting with %X to %d\n", senderId, dataToSend[1], receiverId)
		}
	}
	msgEnd := make([]byte, 101)
	msgEnd[0] = byte(0xFF)
	conn.Write(append(msgEnd))
	conn.Close()
	fmt.Println("Server " + strconv.Itoa(senderId) + " closed connection to " + port + " in sendData")
}


func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Server " + strconv.Itoa(serverId) + ": Got the following server configs:", scs)
	serverNum := len(scs.Servers)
	fmt.Println("Number of servers: ", strconv.Itoa(serverNum))

	//Set up listener
	listenChannel := make(chan []byte)
	defer close(listenChannel)
	thisHost := scs.Servers[serverId].Host
	thisPort := scs.Servers[serverId].Port
	go listenForData(listenChannel, serverId, "tcp", thisHost, thisPort)




	//Read data from our input file
	infileName := os.Args[2]

	infile, err := os.Open(infileName)
	if err != nil {
		log.Fatalf("Server " + strconv.Itoa(serverId) + ": Unable to open %v as input file\n", os.Args[1])
	}

	fileinfo, err := os.Stat(infileName)
	if err != nil {
		log.Fatalf("oops\n")
	}

	filesize := fileinfo.Size()
	d := make([]byte, filesize)
	_, err = infile.Read(d)
	if err != nil {
		log.Fatalf("Server %d: unable to read data.\n")
	}

	infile.Close()
	time.Sleep(500*time.Millisecond)
	//Set up talkers
	var talkers sync.WaitGroup
	fmt.Println("Server " + strconv.Itoa(serverId) + " Starting up connections")
	for s := 0; s < serverNum; s++ {
		if s == serverId {
			continue
		}
		talkers.Add(1)
		go func(S int) {
			neighborHost := string(scs.Servers[S].Host)
			neighborPort := string(scs.Servers[S].Port)
			fmt.Printf("Server %d connecting to %d at %s:%s\n", serverId, S, neighborHost, neighborPort)
			sendData(d, "tcp", serverNum, serverId, S, neighborHost, neighborPort)
			talkers.Done()
		} (s)
	}
//	talkers.Add(1)
//	go func(ch chan<- []byte) {
//		shift := byte(bitsToShift(serverNum))
//		for i := 0; i < len(d); i = i+100 {
//			if int(d[i] >> shift) == serverId {
//				ch <- append([]byte{0x00}, d[i:i+100]...)
//				fmt.Printf("Server %d found its own record with first byte %x\n", serverId, d[i])
//			}
//		}
//		ch <- append([]byte{0xFF}, d[0:100]...)
//		talkers.Done()
//	}(listenChannel)
	talkers.Wait()


	var sievedData []byte
	consolidateServerData(listenChannel, sievedData, serverId, serverNum-1)

	fmt.Printf("Server %d Should be done sieving data!\n", serverId)

//	sort.Sort(registry(sievedData))

//	outfile, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
//	if(err != nil) {
//		log.Fatalf("Unable to open %v as ouput file\n", os.Args[2])
//	}
//	for i:=0; i <num_entries;i++ {
//		_, err = outfile.Write(entries[i])
//		if(err != nil) {
//			log.Fatalf("Unable to write to %v\n", os.Args[2])
//		}
//	}
//	outfile.Close()
}
