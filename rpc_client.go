package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"io"
	"net"
	"sync/atomic"
)

// RPC Client Structure
type Client struct {
	// Request id which is incremented every request, it is used to identify each out-standing
	// request uniquely due to the out of order nature of the client
	id           int64
	conn         net.Conn
	// Channel used by client to send requests to the dispatcher to look for rpc responses
	requestChan  chan *Request
	// Channel used by the reader to send responses to the dispatcher
	responseChan chan *RpcMessage
	// Context indicating whether RPC Client is alive or not
	ctx          context.Context
	cancel       context.CancelFunc
}
/*
	Structure send by the client while making a call to the dispatcher. It fills in the request id and
	the output proto message used for de-serializing. Result is filled into the out message, error is updated.
	Client is indicated if completion by close of the channel.
*/
type Request struct {
	id   int64
	out  proto.Message
	err  error
	done chan struct{}
}

// Create a new client with the given connection, client takes ownership of the network connection.
// Don't do any operation on the socket after creating a client.
func NewClient(conn net.Conn) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{conn: conn, requestChan: make(chan *Request), responseChan: make(chan *RpcMessage, 100), ctx: ctx, cancel: cancel}
	// Launch the reader and the dispatcher in separate routines
	go client.reader()
	go client.dispatcher()
	return client
}

var (
	NilMessageError = errors.New("input or output proto message is nil")
	DispatcherError = errors.New("dispatcher exited")
)
// This method is used to make RPC requests, it is safe to make multiple concurrent requests
func (c *Client) CallMethod(methodName string, input proto.Message, output proto.Message) error {
	var err error
	var anyMessage *any.Any = nil

	if output == nil {
		// Output proto message cannot be nil where as input is permitted to be nil
		return NilMessageError
	}

	if input != nil {
		// If input is not nil, try marshaling into any
		if anyMessage, err = ptypes.MarshalAny(input); err != nil {
			return err
		}
	}

	// Construct the Rpc Message with the request header set
	rpcMessage := &RpcMessage{
		RpcId: atomic.AddInt64(&c.id, 1),
		RpcHeader: &RpcMessage_RequestHeader{
			RequestHeader: &RpcRequestHeader{
				MethodName: methodName,
			},
		},
		Payload: anyMessage,
	}
	// Construct a request entry and sent it across the request channel to the dispatcher
	// for out of responses to be delivered correctly
	request := &Request{
		id:   rpcMessage.GetRpcId(),
		out:  output,
		done: make(chan struct{}),
	}
	select {
	case c.requestChan <- request:
		// Request sent to the dispatcher
	case <-c.ctx.Done():
		// If client is closed or some other error, which resulted in dispatcher exiting
		return DispatcherError
	}
	// Serialize and send the message across the wire
	if err := sendMessage(c.conn, rpcMessage); err != nil {
		return err
	}
	// Wait for request to complete
	select {
	case <-request.done:
		return request.err
	case <-c.ctx.Done():
		// If client is closed or some other error, which resulted in dispatcher exiting
		return DispatcherError
	}

}

/*
	Dispatcher dispatches responses to appropriate rpc requests.
*/
func (c *Client) dispatcher() {
	var err error
	defer func() {fmt.Println("Dispatcher Completed, error:", err)} ()
	mapper := make(map[int64]*Request)
	fmt.Println("Started running the rpc dispatcher.")
	for {
		select {
		case <- c.ctx.Done():
			// If context is completed due to some reason, then exit the dispatcher
			err = c.ctx.Err()
			return
		case request := <-c.requestChan:
			// If any request, then add the to mapper
			// Don't close the request channel for completion, cancel the context rather
			mapper[request.id] = request
		case response := <-c.responseChan:
			// If any RPC response is received, then signal the appropriate rpc request
			request, ok := mapper[response.GetRpcId()]
			if ! ok {
				// If no matching request, then discard it
				fmt.Println("Ignoring Response:", response, "No Matching request")
				continue
			}
			responseHeader := response.GetResponseHeader()
			if responseHeader == nil {
				// Unexpected behaviour: Response Header cannot be nil
				err = errors.New("nil response header")
				return
			}
			if responseHeader.GetStatus() == RpcStatus_ERROR {
				// If some rpc error, then error is set by the server
				request.err = errors.New(fmt.Sprint("RPC SERVER ERROR:", responseHeader.GetError()))
			} else {
				if responseHeader.GetError() != "" {
					// If some application level error
					request.err = errors.New(fmt.Sprint("APP ERROR:", responseHeader.GetError()))
				}
				if response.GetPayload() != nil {
					// If some payload exists
					if err := ptypes.UnmarshalAny(response.GetPayload(), request.out); err != nil {
						// Give priority to un-marshaling error
						request.err = err
					}
				}
			}
			// Indicate completion
			close(request.done)
			// Delete the entry, (Unary RPC)
			delete(mapper, response.GetRpcId())
		}
	}
}

func (c *Client) reader() {
	defer c.cancel()
	var err error
	defer func() { fmt.Println("Client closed connection:", c.conn.Close(), "errors:", err) }()
	framePrefix := make([]byte, 4)
	for {
		// Read the length prefixed frame
		if _, err = io.ReadFull(c.conn, framePrefix); err != nil {
			return
		}
		frameLen := binary.BigEndian.Uint32(framePrefix)
		frame := make([]byte, frameLen)
		if _, err = io.ReadFull(c.conn, frame); err != nil {
			return
		}

		// Un-marshal the rpc response message
		rpcMessage := new(RpcMessage)
		if err = proto.Unmarshal(frame, rpcMessage); err != nil {
			return
		}
		// Send it to the dispatcher
		select {
		case c.responseChan <- rpcMessage:
		case <-c.ctx.Done():
			err = c.ctx.Err()
			return
		}

	}
}

func (c *Client) Close() error {
	// Cancel the context and close the connection.
	c.cancel()
	return c.conn.Close()
}