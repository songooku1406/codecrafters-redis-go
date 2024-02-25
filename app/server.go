package main

import (
    "bufio"
    "fmt"
    "io"
    "net"
    "os"
    "strconv"
    "strings"
    "time"
)

type Value struct {
    val string
    ttl int64 // Unix time in milliseconds when the value will expire
}

type KVStore interface {
    Set(key string, value Value) error
    Get(key string) (Value, error)
}

type KVStoreImpl struct {
    storage map[string]Value
}

func (s *KVStoreImpl) Get(key string) (Value, error) {
    value, ok := s.storage[key]
    if !ok {
        return Value{}, fmt.Errorf("key not found")
    }
    return value, nil
}

func (s *KVStoreImpl) Set(key string, value Value) error {
    s.storage[key] = value
    return nil
}

func NewKVStoreImpl() KVStore {
    return &KVStoreImpl{storage: make(map[string]Value)}
}

func (v *Value) isExpired() bool {
    return v.ttl != -1 && time.Now().UnixMilli() > v.ttl
}

func main() {
    fmt.Println("Logs from your program will appear here!")
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
            continue // Do not exit the program; just move on to the next connection
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
            fmt.Println("Not supported")
            conn.Close() // Close the connection instead of exiting the program
            return
        }
        n, err := reader.ReadString('\n')
        if err != nil {
            return
        }
        numberArgs, err := strconv.Atoi(strings.TrimSpace(n))
        if err != nil {
            fmt.Println("Error parsing number of args:", err)
            return
        }
        var args []string
        for i := 0; i < numberArgs; i++ {
            reader.ReadString('\n') // Read bulk string size
            arg, _ := reader.ReadString('\n')
            args = append(args, strings.TrimSpace(arg))
        }
        handleCommand(writer, args, kvStore)
        writer.Flush()
    }
}

func handleCommand(writer *bufio.Writer, args []string, kvStore KVStore) {
    switch command := strings.ToLower(args[0]); command {
    case "ping":
        writer.WriteString("+PONG\r\n")
    case "echo":
        writer.WriteString(fmt.Sprintf("+%s\r\n", args[1]))
    case "set":
        var expiryTime int64 = -1 // Default: no expiration
        // Look for "px" to set the expiry time
        for i := 0; i < len(args); i++ {
            if strings.ToLower(args[i]) == "px" && i+1 < len(args) {
                // Next argument should be the TTL value
                if et, err := strconv.ParseInt(args[i+1], 10, 64); err == nil {
                    expiryTime = time.Now().UnixMilli() + et
                }
                break // Exit loop once px is found and processed
            }
        }
        v := Value{
            val: args[2],
            ttl: expiryTime,
        }
        kvStore.Set(args[1], v)
        writer.WriteString("+OK\r\n")
    case "get":
        val, err := kvStore.Get(args[1])
        if err != nil || val.isExpired() {
            writer.WriteString("$-1\r\n") // Indicate nil or expired value
        } else {
            writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val.val), val.val))
        }
    default:
        writer.WriteString("ERROR: unknown command\r\n")
    }
}
