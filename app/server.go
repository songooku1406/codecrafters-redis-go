package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"io"
	"strings"
	"strconv"
)

type KVStore interface {
	Set(key string, value string) error
	Get(key string) (string, error)
}

// KVStoreImpl is an implementation of the KVStore interface.
type KVStoreImpl struct {
	storage map[string]string
}

// Get retrieves the value associated with the given key.
func (s *KVStoreImpl) Get(key string) (string, error) {
	value, ok := s.storage[key]
	if !ok {
		return "", nil
	}
	return value, nil
}

// Set sets the value for the given key.
func (s *KVStoreImpl) Set(key string, value string) error {
	s.storage[key] = value
	return nil
}

// NewKVStoreImpl creates a new instance of KVStoreImpl.
func NewKVStoreImpl() KVStore {
	return &KVStoreImpl{storage: make(map[string]string)}
}


func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	kvStore := NewKVStoreImpl()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, kvStore)
	}

}

func handleConnection(conn net.Conn, kvStore KVStore) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		fmt.Println("Reading connection")
		b, err := reader.ReadByte()
		if err == io.EOF {
			return
		}
		if b != '*' {
			fmt.Println(b)
			fmt.Println("Not supported")
			os.Exit(1)
		}
		n, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		numberArgs, _ := strconv.Atoi(n[:len(n)-2])
		var args []string
		for i := 0; i < numberArgs; i++ {
			_, _ = reader.ReadString('\n')
			arg, _ := reader.ReadString('\n')
			args = append(args, arg[:len(arg)-2])
		}
		switch command := strings.ToLower(args[0]); command {
		case "ping":
			_, _ = writer.Write([]byte("+PONG\r\n"))
		case "echo":
			_, _ = writer.Write([]byte(fmt.Sprintf("+%v\r\n", args[1])))
		case "set":
			kvStore.Set(args[1], args[2])
			_, _ = writer.Write([]byte("+OK\r\n"))
		case "get":
			val, _ := kvStore.Get(args[1])
			_, _ = writer.Write([]byte(fmt.Sprintf("+%v\r\n", val)))
		default:
			_, _ = writer.Write([]byte("+PONG\r\n"))
	}
	writer.Flush()
}
}
