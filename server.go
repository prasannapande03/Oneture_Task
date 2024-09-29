package main

import (
    "bufio"
    "log"
    "fmt"
    "net"
    "strconv"
    "strings"
    "sync"
    "time"
    "io"
)

var (
    totalCorrect   int
    totalIncorrect int
    mu             sync.Mutex // Mutex to synchronize access to the counters
)

func handleConnection(conn net.Conn) {
    defer conn.Close()

    // Read the incoming data
    // scanner := bufio.NewScanner(conn)
    reader := bufio.NewReaderSize(conn, 64*1024)
    for {
        message, err := reader.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                log.Println("Client connection closed.")
                break
            }
            log.Println("Error reading from connection :", err)
            break
        }

        result := processMessage(message)

        if result != "" {

            _, err := fmt.Fprintln(conn, result)

            if err != nil {
                log.Println("Error sending response to client: ", err)
            }
        }
    }
    // for scanner.Scan() {
    //     message := scanner.Text()
    //     // Process the message received from the client

    //     // mu.Lock()
    //     result := processMessage(message)
    //     // mu.Unlock()

    //     if result != "" {
    //         // Send the result back to the client
    //         // mu.Lock()
    //         _, err := fmt.Fprintln(conn, result)
    //         if err != nil {
    //             log.Println("Error sending response to client:", err)
    //         }
    //         // mu.Unlock()
    //     }
    // }

    // if err := scanner.Err(); err != nil {
    //     log.Println("Error reading from connection:", err)
    // }
}

func processMessage(message string) string {
    // Split the message by commas
    parts := strings.Split(message, ",")
    if len(parts) != 6 {
        log.Println("Invalid message format:", message)
        return ""
    }

    srNo := parts[0]
    operator := parts[1]
    num1 := strings.TrimSpace(parts[2])
    num2 := strings.TrimSpace(parts[3])
    answer := strings.TrimSpace(parts[4])
    timestamp := parts[5]

    // Log the received message
    log.Printf("Received: SrNo: %s, Operator: %s, Num1: %s, Num2: %s, Answer: %s, Timestamp: %s\n", srNo, operator, num1, num2, answer, timestamp)

    // Calculate the expected answer based on the operator
    var expectedAnswer int
    num1Int, err := strconv.Atoi(num1) // Convert num1 to int
    if err != nil {
        // log.Println("Error converting num1 to int:", err)
        return ""
    }
    
    num2Int, err := strconv.Atoi(num2) // Convert num2 to int
    if err != nil {
        log.Println("Error converting num2 to int:", err)
        return ""
    }

    switch operator {
    case "+":
        expectedAnswer = num1Int + num2Int
    case "-":
        expectedAnswer = num1Int - num2Int
    case "*":
        expectedAnswer = num1Int * num2Int
    case "/":
        if num2Int != 0 {
            expectedAnswer = num1Int / num2Int // Integer division (truncates result)
        } else {
            log.Println("Division by zero, skipping...")
            return ""
        }
    default:
        log.Printf("Unknown operator: %s\n", operator)
        return ""
    }

    // Compare expected answer with the received answer
    receivedAnswer, err := strconv.Atoi(answer)
    if err != nil {
        log.Println("Error converting answer to int:", err)
        return ""
    }

    mu.Lock() // Lock the mutex before updating the counters
    if expectedAnswer == receivedAnswer {
        totalCorrect++
    } else {
        totalIncorrect++
    }
    mu.Unlock()

    correct := expectedAnswer == receivedAnswer

    log.Printf("Total Correct Operations: %d\n", totalCorrect)
    log.Printf("Total Incorrect Operations: %d\n", totalIncorrect)

    // Prepare the result to send back
    result := fmt.Sprintf("%s,%d,%t,%s", srNo, expectedAnswer, correct, time.Now().Format(time.RFC3339))

    return result
}

func main() {
    // Start the server
    ln, err := net.Listen("tcp", ":8080")
    if err != nil {
        log.Fatal("Error starting server:", err)
    }
    defer ln.Close()

    log.Println("Server is listening on port 8080...")
    for {
        conn, err := ln.Accept()
        if err != nil {
            log.Println("Error accepting connection:", err)
            continue
        }
        log.Println("Client connected:", conn.RemoteAddr())

        // Handle the connection in a new goroutine
        go handleConnection(conn)
    }

    // After the server is stopped, print the totals
}