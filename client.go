package main

import (
    "bufio"
    "context"
    "fmt"
    "log"
    "net"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/redis/go-redis/v9"
)

// Structure to hold data to send
type Data struct {
    SrNo      int
    Operator  string
    Num1      int
    Num2      int
    Answer    int
    Timestamp string
}

// Function to send data to the server using a persistent connection
func sendDataToServer(conn net.Conn, data Data) string {
    // Prepare the message to send
    message := fmt.Sprintf("%d,%s,%d,%d,%d,%s\n", data.SrNo, data.Operator, data.Num1, data.Num2, data.Answer, data.Timestamp)

    // Send the message
    _, err := fmt.Fprint(conn, message)
    if err != nil {
        log.Println("Error sending data:", err)
        return ""
    }

    response, err := bufio.NewReader(conn).ReadString('\n')
    if err != nil {
        log.Println("Error receiving response from server:", err)
        return ""
    }

    fmt.Printf("Sent to server: %s", message)
    return response
}

// Worker function to handle operator-specific data processing
func operatorWorker(ctx context.Context, redisClientDB1 *redis.Client, wg *sync.WaitGroup, dataChannel <-chan Data, conn net.Conn) {
    defer wg.Done()
    for data := range dataChannel {
        // Send data and get server response
        response := sendDataToServer(conn, data)
        if response != "" {
            // Split response to update Redis DB 1
            parts := strings.Split(response, ",")
            if len(parts) == 4 {
                srNo, _ := strconv.Atoi(parts[0])
                result, _ := strconv.Atoi(parts[1])
                correct := parts[2]
                timestamp2 := parts[3]
                timestamp3 := time.Now().Format(time.RFC3339)

                // Update Redis DB 1 with the results
                err := redisClientDB1.HSet(ctx, fmt.Sprintf("result:%d", srNo), map[string]interface{}{
                    "Sr. No":      srNo,
                    "result":      result,
                    "correct":     correct,
                    "timestamp2":  timestamp2,
                    "timestamp3":  timestamp3,
                }).Err()
                if err != nil {
                    log.Println("Error updating Redis DB 1:", err)
                } else {
                    log.Printf("Updated Redis DB 1 for SrNo: %d\n", srNo)
                }
            }
        }
    }
}

// Main function to read data from Redis DB 0 and dispatch it to operator-specific workers
func main() {
    ctx := context.Background()

    // Redis client for DB 0
    redisClientDB0 := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379", // Redis server address
        DB:   0,                // Using DB 0 for reading
    })

    // Redis client for DB 1
    redisClientDB1 := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379", // Redis server address
        DB:   8,                // Using DB 1 for writing
    })

    var wg sync.WaitGroup

    // Create channels for each operation type
    additionChannel := make(chan Data, 100)
    subtractionChannel := make(chan Data, 100)
    multiplicationChannel := make(chan Data, 100)
    divisionChannel := make(chan Data, 100)

    // Establish persistent connections for each operator
    additionConn, err := net.Dial("tcp", "localhost:8080")
    if err != nil {
        log.Fatal("Error connecting for addition:", err)
    }
    subtractionConn, err := net.Dial("tcp", "localhost:8080")
    if err != nil {
        log.Fatal("Error connecting for subtraction:", err)
    }
    multiplicationConn, err := net.Dial("tcp", "localhost:8080")
    if err != nil {
        log.Fatal("Error connecting for multiplication:", err)
    }
    divisionConn, err := net.Dial("tcp", "localhost:8080")
    if err != nil {
        log.Fatal("Error connecting for division:", err)
    }

    // Start 4 workers (one for each operator type) using persistent connections
    wg.Add(4)
    go operatorWorker(ctx, redisClientDB1, &wg, additionChannel, additionConn)
    go operatorWorker(ctx, redisClientDB1, &wg, subtractionChannel, subtractionConn)
    go operatorWorker(ctx, redisClientDB1, &wg, multiplicationChannel, multiplicationConn)
    go operatorWorker(ctx, redisClientDB1, &wg, divisionChannel, divisionConn)

    // Adjust the range according to the number of records you have
    for srNo := 1; srNo <= 1000000; srNo++ {
        recordKey := fmt.Sprintf("%d", srNo)

        // Fetch the full string from Redis DB 0
        redisData, err := redisClientDB0.Get(ctx, recordKey).Result()
        if err != nil {
            log.Println("Error fetching data from Redis DB 0:", err)
            continue
        }

        // Split the redisData string into its components (Operator, Num1, Num2, Answer)
        dataParts := strings.Split(redisData, ",") // Assuming the data is stored in CSV format
        if len(dataParts) != 4 {
            log.Printf("Invalid data format for record %d: %s\n", srNo, redisData)
            continue
        }

        // Parse the data
        operator := strings.TrimSpace(dataParts[0])
        num1, err := strconv.Atoi(strings.TrimSpace(dataParts[1]))
        if err != nil {
            log.Println("Error converting num1 to int:", err)
            continue
        }

        num2, err := strconv.Atoi(strings.TrimSpace(dataParts[2]))
        if err != nil {
            log.Println("Error converting num2 to int:", err)
            continue
        }

        answer, err := strconv.Atoi(strings.TrimSpace(dataParts[3])) // Changed to int
        if err != nil {
            log.Println("Table entry in the answer: ", dataParts[3])
            log.Println("Error converting answer to int:", err) // Updated error message
            continue
        }

        // Prepare data to send
        data := Data{
            SrNo:      srNo,
            Operator:  operator,
            Num1:      num1,
            Num2:      num2,
            Answer:    answer, // Now integer
            Timestamp: time.Now().Format(time.RFC3339), // Current timestamp
        }

        // Dispatch data to the appropriate channel based on the operator
        switch operator {
        case "+":
            additionChannel <- data
        case "-":
            subtractionChannel <- data
        case "*":
            multiplicationChannel <- data
        case "/":
            if num2 != 0 {
                data.Answer = num1 / num2 // Integer division
                divisionChannel <- data
            } else {
                log.Printf("Division by zero for record %d: %s\n", srNo, operator)
            }
        default:
            log.Printf("Unsupported operator for record %d: %s\n", srNo, operator)
        }
    }

    // Close all channels (this will signal the workers to stop)
    close(additionChannel)
    close(subtractionChannel)
    close(multiplicationChannel)
    close(divisionChannel)

    // Wait for all workers to finish
    wg.Wait()

    // Close all connections
    additionConn.Close()
    subtractionConn.Close()
    multiplicationConn.Close()
    divisionConn.Close()

    fmt.Println("All data sent to the server and updated in Redis DB 1.")
}
