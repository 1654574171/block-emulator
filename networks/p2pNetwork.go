package networks

import (
	"blockEmulator/params"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"math/rand"

	"golang.org/x/time/rate"
)

var connMaplock sync.Mutex
var connectionPool = make(map[string]net.Conn, 0)

// network params.
var randomDelayGenerator *rand.Rand
var rateLimiterDownload *rate.Limiter
var rateLimiterUpload *rate.Limiter

// Define the latency, jitter and bandwidth here.
// Init tools.
func InitNetworkTools() {
	// avoid wrong params.
	if params.Delay < 0 {
		params.Delay = 0
	}
	if params.JitterRange < 0 {
		params.JitterRange = 0
	}
	if params.Bandwidth < 0 {
		params.Bandwidth = 0x7fffffff
	}

	// generate the random seed.
	randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixMicro()))
	// Limit the download rate
	rateLimiterDownload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
	// Limit the upload rate
	rateLimiterUpload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
}

// 重连逻辑抽取成独立的函数
func reconnectToServer(addr string) (net.Conn, error) {
	var conn net.Conn
	var err error

	// 尝试重连并返回新的连接
	for retry := 0; retry < 5; retry++ {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			log.Printf("Reconnected to %s on attempt %d\n", addr, retry+1)
			return conn, nil
		}

		// 如果重连失败，等待并继续重试
		log.Printf("Reconnect attempt %d failed for %s, error: %v\n", retry+1, addr, err)
		time.Sleep(1000 * time.Millisecond) // 每次重试间隔 1 秒
	}

	return nil, fmt.Errorf("failed to reconnect to %s after 5 attempts", addr)
}

// tcodial非go routine版本
func TcpDialNonGoRoutine(context []byte, addr string) {
	go func() {
		// 模拟延迟
		thisDelay := params.Delay
		if params.JitterRange != 0 {
			thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
		}
		time.Sleep(time.Millisecond * time.Duration(thisDelay))

		// connMaplock.Lock()
		// defer connMaplock.Unlock()

		var err error
		var conn net.Conn

		// 如果连接池中已有连接，使用该连接
		if c, ok := connectionPool[addr]; ok {
			if tcpConn, tcpOk := c.(*net.TCPConn); tcpOk {
				// 检查连接是否有效
				if err := tcpConn.SetKeepAlive(true); err != nil {
					// 如果连接不可用，删除并尝试重连
					delete(connectionPool, addr)

					// 调用重连函数
					conn, err = reconnectToServer(addr)
					if err != nil {
						log.Println("Reconnect error", err)
						return
					}
					connectionPool[addr] = conn
					go ReadFromConn(addr) // 启动新连接的读取协程
				} else {
					// 使用现有连接
					conn = c
				}
			}
		} else {
			// 连接池中没有连接，建立新的连接
			conn, err = reconnectToServer(addr)
			if err != nil {
				log.Println("Connect error", err)
				return
			}
			connectionPool[addr] = conn
			go ReadFromConn(addr) // 启动新连接的读取协程
		}

		// 发送数据，如果发送失败则重连
		err = writeToConn(append(context, '\n'), conn, rateLimiterUpload)
		maxRetry := 5 // 最大重试次数
		retryCount := 0

		// 重连并发送数据的循环
		for {
			if err == nil {
				break // 发送成功，退出循环
			}

			log.Printf("Send failed, retrying... (Attempt %d/%d)\n", retryCount+1, maxRetry)

			// 达到最大重试次数后，退出
			if retryCount >= maxRetry {
				log.Println("Max retries reached, giving up.")
				break
			}

			// 休眠一段时间再重试
			time.Sleep(1000 * time.Millisecond)
			retryCount++

			// 尝试重连
			conn, err = reconnectToServer(addr)
			if err != nil {
				log.Println("Reconnect error", err)
				continue // 如果重连失败，继续重试
			}

			// 如果重连成功，更新连接池并重试发送
			connectionPool[addr] = conn
			go ReadFromConn(addr) // 启动新连接的读取协程
			err = writeToConn(append(context, '\n'), conn, rateLimiterUpload)
		}
	}()
}
func TcpDial(context []byte, addr string) {
	go func() {
		// 模拟延迟
		thisDelay := params.Delay
		if params.JitterRange != 0 {
			thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
		}
		time.Sleep(time.Millisecond * time.Duration(thisDelay))

		connMaplock.Lock()
		defer connMaplock.Unlock()

		var err error
		var conn net.Conn

		// 如果连接池中已有连接，使用该连接
		if c, ok := connectionPool[addr]; ok {
			if tcpConn, tcpOk := c.(*net.TCPConn); tcpOk {
				// 检查连接是否有效
				if err := tcpConn.SetKeepAlive(true); err != nil {
					// 如果连接不可用，删除并尝试重连
					delete(connectionPool, addr)

					// 调用重连函数
					conn, err = reconnectToServer(addr)
					if err != nil {
						log.Println("Reconnect error", err)
						return
					}
					connectionPool[addr] = conn
					go ReadFromConn(addr) // 启动新连接的读取协程
				} else {
					// 使用现有连接
					conn = c
				}
			}
		} else {
			// 连接池中没有连接，建立新的连接
			conn, err = reconnectToServer(addr)
			if err != nil {
				log.Println("Connect error", err)
				return
			}
			connectionPool[addr] = conn
			go ReadFromConn(addr) // 启动新连接的读取协程
		}

		// 发送数据，如果发送失败则重连
		err = writeToConn(append(context, '\n'), conn, rateLimiterUpload)
		maxRetry := 5 // 最大重试次数
		retryCount := 0

		// 重连并发送数据的循环
		for {
			if err == nil {
				break // 发送成功，退出循环
			}

			log.Printf("Send failed, retrying... (Attempt %d/%d)\n", retryCount+1, maxRetry)

			// 达到最大重试次数后，退出
			if retryCount >= maxRetry {
				log.Println("Max retries reached, giving up.")
				break
			}

			// 休眠一段时间再重试
			time.Sleep(1000 * time.Millisecond)
			retryCount++

			// 尝试重连
			conn, err = reconnectToServer(addr)
			if err != nil {
				log.Println("Reconnect error", err)
				continue // 如果重连失败，继续重试
			}

			// 如果重连成功，更新连接池并重试发送
			connectionPool[addr] = conn
			go ReadFromConn(addr) // 启动新连接的读取协程
			err = writeToConn(append(context, '\n'), conn, rateLimiterUpload)
		}
	}()
}

// Broadcast sends a message to multiple receivers, excluding the sender.
func Broadcast(sender string, receivers []string, msg []byte) {
	for _, ip := range receivers {
		if ip == sender {
			continue
		}
		go TcpDial(msg, ip)
	}
}

// CloseAllConnInPool closes all connections in the connection pool.
func CloseAllConnInPool() {
	connMaplock.Lock()
	defer connMaplock.Unlock()

	for _, conn := range connectionPool {
		conn.Close()
	}
	connectionPool = make(map[string]net.Conn) // Reset the pool
}

// ReadFromConn reads data from a connection.
func ReadFromConn(addr string) {
	conn := connectionPool[addr]

	// new a conn reader
	connReader := NewConnReader(conn, rateLimiterDownload)

	buffer := make([]byte, 1024)
	var messageBuffer bytes.Buffer

	for {
		n, err := connReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error for address", addr, ":", err)
			}
			break
		}

		// add message to buffer
		messageBuffer.Write(buffer[:n])

		// handle the full message
		for {
			message, err := readMessage(&messageBuffer)
			if err == io.ErrShortBuffer {
				// continue to load if buffer is short
				break
			} else if err == nil {
				// log the full message
				log.Println("Received from", addr, ":", message)
			} else {
				// handle other errs
				log.Println("Error processing message for address", addr, ":", err)
				break
			}
		}
	}
}

func readMessage(buffer *bytes.Buffer) (string, error) {
	message, err := buffer.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(message), nil
}
