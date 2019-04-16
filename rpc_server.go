package rpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"io"
	"net"
	"reflect"
)

type Method struct {
	function reflect.Value
	inParam reflect.Type
	outParam reflect.Type
}

type Server struct {
	mapper map[string]*Method
}
var (
	NotAFunctionError = errors.New("not a function")
	InvalidFunctionError = errors.New("invalid function, does not have expected arguments")
	NoRequestHeaderError = errors.New("missing request header")
)


func (s *Server) Register(name string, method interface{}) error {
	methodType := reflect.TypeOf(method)
	if methodType == nil || methodType.Kind() != reflect.Func {
		return NotAFunctionError
	}
	if methodType.NumIn() != 1 || methodType.NumOut() != 2 ||
		!methodType.In(0).ConvertibleTo(reflect.TypeOf((*proto.Message)(nil)).Elem()) ||
		!methodType.Out(0).ConvertibleTo(reflect.TypeOf((*proto.Message)(nil)).Elem()) ||
		methodType.Out(1) != reflect.TypeOf((*error)(nil)).Elem() ||
		methodType.In(0).Kind() == reflect.Interface ||
		methodType.Out(0).Kind() == reflect.Interface{
		return InvalidFunctionError
	}
	// Replaces existing method
	s.mapper[name] = &Method{
		function: reflect.ValueOf(method),
		inParam: methodType.In(0),
		outParam: methodType.Out(0),
	}
	return nil
}

func sendMessage(conn net.Conn, message *RpcMessage) error {
	marshaled, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	framePrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(framePrefix, uint32(len(marshaled)))
	wireFormat := bytes.Join([][]byte{framePrefix, marshaled}, nil)
	_, err = conn.Write(wireFormat)
	return err
}
/*
	Handles the given rpc request message. If method exists, then calls it with specified inputs
	and writes the response.
*/
func (s *Server) handleRequest(conn net.Conn, message *RpcMessage) {
	requestHeader := message.GetRequestHeader()
	name := requestHeader.GetMethodName()
	method, ok := s.mapper[name]
	if !ok {
		// If method does not exist, then return a RPC level error
		message.RpcHeader = &RpcMessage_ResponseHeader{
			ResponseHeader:  &RpcResponseHeader{
				// RPC Error
				Status: RpcStatus_ERROR,
				Error: "Method Not Found",
			},
		}
		// RPC Level errors don't have payloads
		message.Payload = nil
		sendMessage(conn, message)
		return
	}
	var inputParam proto.Message
	if message.GetPayload() != nil {
		// If input payload is sent, try un-marshalling it into the expected input type
		inputParam = reflect.New(method.inParam.Elem()).Interface().(proto.Message)
		if err := ptypes.UnmarshalAny(message.Payload, inputParam); err != nil {
			// If could not un-marshal, the potentially passed input may have been of different type
			message.RpcHeader = &RpcMessage_ResponseHeader{
				ResponseHeader:  &RpcResponseHeader{
					Status: RpcStatus_ERROR,
					Error: "Mismatch Input Type",
				},
			}
			message.Payload = nil
			sendMessage(conn, message)
			return
		}
	} else {
		// If no input parameter, create a nil value of that type to pass the the method by reflection
		inputParam = reflect.New(method.inParam).Elem().Interface().(proto.Message)
	}
	// Call the method
	out := method.function.Call([]reflect.Value{reflect.ValueOf(inputParam)})
	message.RpcHeader = &RpcMessage_ResponseHeader{
		ResponseHeader:  &RpcResponseHeader{
			// There are no RPC level headers
			Status: RpcStatus_OK,
		},
	}
	if !out[1].IsNil() {
		// Set the application level error, if any error
		message.GetResponseHeader().Error = out[1].Interface().(error).Error()
	}
	if !out[0].IsNil() {
		// If any output it returned, then marshal it and send it
		if any, err:=  ptypes.MarshalAny(out[0].Interface().(proto.Message)); err != nil {
			message.GetResponseHeader().Error = err.Error()
		} else {
			message.Payload = any
		}
	}
	sendMessage(conn, message)
}
// This function is used to handle RPC Requests from the given connection
func (s *Server) ServeConnection(conn net.Conn) error {
	framePrefix := make([]byte, 4)
	for {
		// Read the length-prefixed frame
		if _, err:= io.ReadFull(conn, framePrefix); err != nil {
			return nil
		}
		frameLen := binary.BigEndian.Uint32(framePrefix)
		frame := make([]byte, frameLen)
		if _, err:= io.ReadFull(conn, frame); err != nil {
			return nil
		}
		// Unmarshal it into the RPC Message
		rpcMessage := new(RpcMessage)
		if err := proto.Unmarshal(frame,rpcMessage); err != nil {
			return err
		}

		if  rpcMessage.GetRequestHeader() == nil {
			// No request header, then return error
			return NoRequestHeaderError
		}
		go s.handleRequest(conn, rpcMessage)
	}
}

func (s *Server) Serve(lis net.Listener) error {
	// Start the RPC Server on the given listener socket,
	// Currently there is no way for to terminate the server, apart from closing the listening socket.
	// However any established connections will proceed till clients terminate
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		fmt.Println("Accepted connection from:", conn.RemoteAddr())
		// For each connection, serve to the requests
		go func(conn net.Conn) { fmt.Println("ERROR:", s.ServeConnection(conn)) } (conn)
	}
}

func NewServer() *Server {
	server := &Server {
		mapper: make(map[string]*Method),
	}
	return server
}
