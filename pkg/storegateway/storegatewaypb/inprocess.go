package storegatewaypb

import (
	"context"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
	"io"
)

func NewServerAsClient(srv storepb.StoreServer, clientReceiveBufferSize int) StoreGatewayClient {
	return &ServerAsClient{srv: srv, clientReceiveBufferSize: clientReceiveBufferSize}
}

// serverAsClient allows to use servers as clients.
// NOTE: Passing CallOptions does not work - it would be needed to be implemented in grpc itself (before, after are private).
type ServerAsClient struct {
	srv storepb.StoreServer

	clientReceiveBufferSize int
}

func (s ServerAsClient) RemoteAddress() string {
	return "local"
}

func (s ServerAsClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, nil
}

func (s ServerAsClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return nil, nil
}

func (s ServerAsClient) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (StoreGateway_QueryClient, error) {
	return nil, nil
}

func (s ServerAsClient) QueryRange(ctx context.Context, in *QueryRangeRequest, opts ...grpc.CallOption) (StoreGateway_QueryRangeClient, error) {
	return nil, nil
}

func (s ServerAsClient) Series(ctx context.Context, in *storepb.SeriesRequest, _ ...grpc.CallOption) (StoreGateway_SeriesClient, error) {
	inSrv := &inProcessStream{recv: make(chan *storepb.SeriesResponse, s.clientReceiveBufferSize), err: make(chan error)}
	inSrv.ctx, inSrv.cancel = context.WithCancel(ctx)
	go func() {
		inSrv.err <- s.srv.Series(in, inSrv)
		close(inSrv.err)
		close(inSrv.recv)
	}()
	return &inProcessClientStream{srv: inSrv}, nil
}

// TODO(bwplotka): Add streaming attributes, metadata etc. Currently those are disconnected. Follow up on https://github.com/grpc/grpc-go/issues/906.
// TODO(bwplotka): Use this in proxy.go and receiver multi tenant proxy.
type inProcessStream struct {
	grpc.ServerStream

	ctx    context.Context
	cancel context.CancelFunc
	recv   chan *storepb.SeriesResponse
	err    chan error
}

func (s *inProcessStream) Context() context.Context { return s.ctx }

func (s *inProcessStream) Send(r *storepb.SeriesResponse) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.recv <- r:
		return nil
	}
}

type inProcessClientStream struct {
	grpc.ClientStream

	srv *inProcessStream
}

func (s *inProcessClientStream) Context() context.Context { return s.srv.ctx }

func (s *inProcessClientStream) CloseSend() error {
	s.srv.cancel()
	return nil
}

func (s *inProcessClientStream) Recv() (*storepb.SeriesResponse, error) {
	select {
	case <-s.srv.ctx.Done():
		return nil, s.srv.ctx.Err()
	case r, ok := <-s.srv.recv:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case err := <-s.srv.err:
		if err == nil {
			return nil, io.EOF
		}
		return nil, err
	}
}
