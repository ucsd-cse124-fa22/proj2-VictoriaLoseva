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
)

type msg [101]byte

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
		if(message[0] == byte(1)) {
			numOfClientsCompleted++
			fmt.Println("Server ", serverId, " Found an end of stream msg")
		}
		data = append(data, message[1:101]...)
		fmt.Println("Server " + strconv.Itoa(serverId) + "Got message starting with " + strconv.Itoa(int(message[1])) + " completed " + strconv.Itoa(numOfClientsCompleted) + " clients")
	}
}

//func dataSieve(r []byte, binId byte) {
//	if(byte(r[0]) &  byte(12) == binId) {
//		return true
//	}
//	return false
//}

func sendData(data []byte, cType string, serverId int, host string, port string) {
	fmt.Println("Server " + strconv.Itoa(serverId) + " requesting " + cType + " connection on connHost: " + host + ", connPort: " + port)
	conn, err := net.Dial(cType, host+":"+port)
	for err != nil {
		time.Sleep(250*time.Millisecond)
		conn, err = net.Dial(cType, host+":"+port)
	}
	fmt.Println("Server " + strconv.Itoa(serverId) + " connected to" + host + ":" + port)
	defer conn.Close()
	dataToSend := append([]byte{0xFF}, data[serverId*100:(serverId+1)*100]...)
	conn.Write(dataToSend)
	fmt.Println("Server " + strconv.Itoa(serverId) + " sent message starting with " + strconv.Itoa(int(dataToSend[0])))
	conn.Close()
	fmt.Println("Server " + strconv.Itoa(serverId) + " closed connection to " + port + " in sendData")
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




	//Handwritten Server id's
	binId := byte(0)
	if(serverId == 0) {
		binId = byte(0x0)
	} else if(serverId == 1) {
		binId = byte(0x4)
	} else if(serverId == 2) {
		binId = byte(0x8)
	} else {
		binId =  byte(0xC)
	}

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
	infile.Read(d)

	infile.Close()

	//Set up talkers
	var talkers sync.WaitGroup
	fmt.Println("Server " + strconv.Itoa(serverId) + " Starting up connections")
	for s := 0; s < serverNum; s++ {
		fmt.Println("Loop: " + strconv.Itoa(s))
		if s != serverId {
			go func(S int) {
				defer talkers.Done()
			fmt.Println("Server " + strconv.Itoa(serverId) + " connecting to " + strconv.Itoa(S))
			neighborHost := string(scs.Servers[S].Host)
			neighborPort := string(scs.Servers[S].Port)
			talkers.Add(1)
			sendData(d, "tcp", S, neighborHost, neighborPort)
			} (s)
		}
	}
	talkers.Wait()
	//Go through all the records, print out the ones we don't want\

	for i:= 0; i < int(filesize); i=i+100 {
		if(byte(d[i]) &  byte(12) != binId) {
//			fmt.Printf("Server %x: removing %x\n", serverId, byte(d[i]) &  byte(12))
		}
	}
	
	sievedData := make([]byte, filesize)
	consolidateServerData(listenChannel, sievedData, serverId, serverNum-1)

	fmt.Println("Should be done!")

}
