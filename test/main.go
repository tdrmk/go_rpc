package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/tdineshramkumar/perf_client"
	"log"
	"net"
	"runtime"
	"time"
)
import "github.com/tdineshramkumar/rpc"
var (
	isServer = flag.Bool("server", false, "run as client or server")
	serverAddress = flag.String("address", "127.0.0.1:8091", "the server network address")
	gomaxprocs = flag.Int("gomaxprocs", 0, "the max procs to use")
)

func Echo(request *Request) (*Response, error){
	if request == nil {
		return nil, errors.New("empty request")
	}
	return &Response{Message: request.Message}, nil
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*gomaxprocs)
	fmt.Println("GOMAXPROCS:", *gomaxprocs)
	if *isServer {
		lis, err := net.Listen("tcp", *serverAddress)
		if err != nil {
			log.Fatalln("Listen Failed:", err)
		}
		server := rpc.NewServer()
		log.Println("Register Echo, errors:", server.Register("echo", Echo))
		log.Fatalln(server.Serve(lis))
	} else {
		conn, err := net.Dial("tcp", *serverAddress)
		if err != nil {
			log.Fatalln("Dial Failed:", err)
		}
		request := &Request{Message: "hello"}
		response := &Response{Message:"Hi"}
		client := rpc.NewClient(conn)
		fmt.Println("error method:", client.CallMethod("error", request, response))
		fmt.Println("echo method:", client.CallMethod("echo", request, response))
		fmt.Println("echo method:", client.CallMethod("echo", request, request))
		fmt.Println("echo method:", client.CallMethod("echo", nil, nil))
		time.Sleep(5 * time.Second)
		numRoutines := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
		for _, numRoutine := range numRoutines {
			task := perf_client.Task(func() error{return client.CallMethod("echo", request, response)})
			fmt.Println(perf_client.RunPerfTest(task, time.Second * 60, numRoutine))
			time.Sleep(5 * time.Second)
		}

	}
}
