// The grpc-go arm of the local integration race: bench/integration/README.md.
//
// It serves bench.v1.Bench/Unary with a constant reply and never looks at the
// request, so a round measures transport and framing and nothing else. The
// service is registered by hand and the codec is raw bytes, so the arm runs
// the stock grpc-go server without protoc or generated code.
//
// Usage: go run . [port] [reply-size], defaults 50152 and 8.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"
)

// The same stream limit every arm advertises (README, "Configuration
// parity"). Above every profile in the matrix, so no arm throttles.
const maxStreams = 1000

// raw passes the message bytes through untouched: the handler's reply is the
// wire message already, and the request is never decoded. It is named "proto"
// so the response content-type is wire-identical to the other arms.
type raw struct{}

func (raw) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("raw codec: expected []byte, got %T", v)
	}
	return b, nil
}

func (raw) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("raw codec: expected *[]byte, got %T", v)
	}
	*b = data
	return nil
}

func (raw) Name() string { return "proto" }

// compose returns the serialized bench.v1.Payload with size bytes: field 1,
// wire type 2, then the length-prefixed data. Same bytes the other arms send.
func compose(size int) []byte {
	out := make([]byte, 0, size+5)
	out = append(out, 0x0A)
	v := size
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	out = append(out, byte(v))
	for range size {
		out = append(out, 'a')
	}
	return out
}

type server struct{}

func (server) unary(
	ctx context.Context,
	req *[]byte,
) ([]byte, error) {
	return reply, nil
}

var reply []byte

// The handler shape grpc-go's generated code would install: dec is the
// request codec call, and the return value goes back through the codec. No
// interceptor is registered, so none is invoked.
func unaryHandler(
	srv any,
	ctx context.Context,
	dec func(any) error,
	_ grpc.UnaryServerInterceptor,
) (any, error) {
	in := new([]byte)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(benchServer).unary(ctx, in)
}

type benchServer interface {
	unary(context.Context, *[]byte) ([]byte, error)
}

var srv server

func main() {
	port := 50152
	size := 8
	if len(os.Args) > 1 {
		if p, err := strconv.Atoi(os.Args[1]); err == nil {
			port = p
		}
	}
	if len(os.Args) > 2 {
		if s, err := strconv.Atoi(os.Args[2]); err == nil {
			size = s
		}
	}
	reply = compose(size)

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.ForceServerCodec(raw{}),
		grpc.MaxConcurrentStreams(maxStreams),
	)
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "bench.v1.Bench",
		HandlerType: (*benchServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Unary",
			Handler:    unaryHandler,
		}},
		Metadata: "bench.proto",
	}, &srv)

	fmt.Printf("go arm listening on 127.0.0.1:%d, %d byte replies\n", port, size)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
