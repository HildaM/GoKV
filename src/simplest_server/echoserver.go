package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

func ListenAndServer(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen err: %v", err))
	}
	defer listener.Close()

	log.Println(fmt.Sprintf("bind: %s, start listening...", address))

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(fmt.Sprintf("accept err: %v", err))
		}
		go Handle(conn)
	}
}

func Handle(conn net.Conn) {
	// bufio: 提供缓冲区的io流
	reader := bufio.NewReader(conn)
	for {
		// 一直阻塞，直到读到'\n'
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("connection close")
			} else {
				log.Println(err)
			}
			return
		}
		b := []byte(msg)
		// 将收到的信息发给客户端
		conn.Write(b)
	}
}

func main() {
	ListenAndServer(":8000")
}
