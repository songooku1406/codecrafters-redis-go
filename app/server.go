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
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
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
		default:
			_, _ = writer.Write([]byte("+PONG\r\n"))
	}
	writer.Flush()
}
}
