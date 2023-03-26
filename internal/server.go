package internal

import (
	"fmt"
	v1 "github.com/Big-Kotik/transparent-data-bridge-api/bridge/api/v1"
	"github.com/rs/zerolog/log"
	"io"
	"sync"
)

type RelayServer struct {
	v1.UnimplementedTransparentDataRelayServiceServer
	v1.UnimplementedTransparentDataBridgeServiceServer

	m       sync.RWMutex
	servers map[int32]chan *v1.SendFileRequest
	// We are using here FileName because it's uuid
	// TODO: delete from map
	requests map[string]chan *v1.FileChunk
}

func NewRelayServer() *RelayServer {
	return &RelayServer{
		servers:  make(map[int32]chan *v1.SendFileRequest),
		requests: make(map[string]chan *v1.FileChunk),
	}
}

func (r *RelayServer) RegisterServer(auth *v1.Auth, server v1.TransparentDataRelayService_RegisterServerServer) error {
	ch, err := r.registerServer(auth.Id)

	log.Info().Msgf("new server registered %d", auth.Id)

	if err != nil {
		return err
	}

	for req := range ch {
		log.Debug().Msgf("get request %s to server %d", req.GetFileName(), req.GetDestination())
		err := server.Send(req)
		if err != nil {
			r.deregisterServer(auth.Id)
			return err
		}
		log.Debug().Msgf("chunk sended %s to server %d", req.GetFileName(), req.GetDestination())
	}

	return nil
}

func (r *RelayServer) ReceiveChunks(request *v1.SendFileRequest, server v1.TransparentDataRelayService_ReceiveChunksServer) error {
	r.m.RLock()
	ch, ok := r.requests[request.FileName]
	r.m.RUnlock()

	if !ok {
		return fmt.Errorf("can't find file by it's name")
	}

	log.Info().Msgf("start download file %s", request.FileName)

	for chunk := range ch {
		err := server.Send(chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RelayServer) registerServer(id int32) (<-chan *v1.SendFileRequest, error) {
	ch := make(chan *v1.SendFileRequest, 10)

	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.servers[id]; ok {
		return nil, fmt.Errorf("%d already registered", id)
	}
	r.servers[id] = ch

	return ch, nil
}

func (r *RelayServer) deregisterServer(id int32) {
	r.m.Lock()
	defer r.m.Unlock()

	if ch, ok := r.servers[id]; ok {
		if _, open := <-ch; open {
			close(ch)
		}
	}

	delete(r.servers, id)
}

func (r *RelayServer) registerRequest(req *v1.SendFileRequest) (chan<- *v1.FileChunk, error) {
	ch := make(chan *v1.FileChunk, 10)

	r.m.Lock()
	defer r.m.Unlock()

	server, ok := r.servers[req.Destination]
	if !ok {
		return nil, fmt.Errorf("can't find server with id: %d", req.Destination)
	}

	log.Debug().Msg("sending to chan")
	server <- req
	log.Debug().Msg("sent to chan")

	if _, ok := r.requests[req.FileName]; ok {
		return nil, fmt.Errorf("%s already sending file", req.FileName)
	}
	r.requests[req.FileName] = ch

	return ch, nil
}

func (r *RelayServer) deregisterRequest(id string) {
	r.m.Lock()
	defer r.m.Unlock()

	if ch, ok := r.requests[id]; ok {
		if _, open := <-ch; open {
			close(ch)
		}
	}

	delete(r.requests, id)
}

func (r *RelayServer) SendChunks(server v1.TransparentDataBridgeService_SendChunksServer) error {
	file, err := server.Recv()
	if err == io.EOF {
		return fmt.Errorf("unexpected EOF")
	} else if err != nil {
		return err
	}

	fi := file.GetRequest()
	if fi == nil {
		return fmt.Errorf("first request must be SendFileRequest")
	}

	log.Info().Msgf("register new reuqest %s to %d", fi.GetFileName(), fi.GetDestination())

	ch, err := r.registerRequest(fi)
	defer func() {
		r.deregisterRequest(fi.FileName)
		log.Info().Msgf("deregister new request %s to %d", fi.GetFileName(), fi.GetDestination())
	}()

	for {
		file, err := server.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		chunk := file.GetChunk()
		if chunk == nil {
			return fmt.Errorf("expected chunk")
		}

		ch <- chunk
	}

	if err := server.SendAndClose(&v1.FileStatus{LastChunkOffset: 0}); err != nil {
		return err
	}

	return nil
}

func (r *RelayServer) Stop() {
	log.Info().Msg("stopping relay server")
	r.m.Lock()
	defer r.m.Unlock()
	log.Info().Msg("locked relay server")

	for k, v := range r.servers {
		_, open := <-v
		log.Info().Msgf("%b", open)
		if open {
			log.Info().Msgf("close %d", k)
			close(v)
		}
		delete(r.servers, k)
	}

	for k, v := range r.requests {
		if _, open := <-v; open {
			close(v)
		}
		delete(r.requests, k)
	}
	log.Info().Msg("relay server stopped")
}
