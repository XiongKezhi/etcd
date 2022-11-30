package main

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/v3/contrib/raftexample/mergepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"log"
	"net"
	"strings"
)

type mergeServer struct {
	rc            *raftNode
	mergeProposeC chan<- proposal
	mergepb.UnimplementedMergeServer
}

// serveRpcMergeAPI starts a merge server to server RPC calls.
func serveRpcMergeAPI(rc *raftNode, mergePort int, mergeProposeC chan<- proposal) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", mergePort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	mergepb.RegisterMergeServer(srv, &mergeServer{rc: rc, mergeProposeC: mergeProposeC})
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

const batchSize = 128                // number of logs in one batch
const maxSize = 18446744073709551615 // max size in bytes

func (m *mergeServer) GetLogs(ctx context.Context, in *mergepb.LogRequest) (
	*mergepb.LogResponse, error) {
	log.Printf("Received request for log starting from %v", in.Index)

	// get all following logs, shouldn't be performance problem since the method returns a slice (a reference)
	ents, err := m.rc.raftStorage.Entries(in.Index, m.rc.node.Status().Commit+1, maxSize)
	if err != nil {
		if err == raft.ErrCompacted {
			// TODO: send snapshot
		}
		return nil, err
	}

	// retrieve data from normal entries
	data := make([][]byte, 0, batchSize)
	for i := range ents {
		// only transfer logs for kvstore
		if ents[i].Type == raftpb.EntryNormal && len(ents[i].Data) != 0 {
			data = append(data, ents[i].Data)
		}

		// enough logs, return one batch
		if len(data) >= batchSize {
			return &mergepb.LogResponse{Logs: data, LastBatch: false}, nil
		}
	}

	// get client ip
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("get client ip from context failed")
	}
	ip := p.Addr.String()[:strings.LastIndex(p.Addr.String(), ":")] // remove :port in the end of address

	// not enough for one batch, propose to redirect future requests
	msg := mergepb.MergeMessage{Id: in.Id, Type: mergepb.MergeRedirect, RedirectIp: ip, RedirectPort: in.KvPort}
	msgBytes, err := msg.Marshal()
	if err != nil {
		return nil, err
	}

	// propose and wait on commit and apply
	wc := make(chan error)
	m.mergeProposeC <- proposal{Message: string(msgBytes), wait: wc}
	if err := <-wc; err != nil && !errors.Is(err, raft.ErrStopped) {
		log.Printf("Propose last batch failed: %v", err)
		return nil, fmt.Errorf("propose last batch error: %v", err)
	}

	return &mergepb.LogResponse{Logs: data, LastBatch: true}, nil
}

func (m *mergeServer) Refresh(ctx context.Context, in *mergepb.RefreshRequest) (
	*mergepb.RefreshResponse, error) {
	log.Printf("Received request from node #%v to refresh node", in.RequesterId)

	peers := make(map[int]string)
	for k, v := range in.Peers {
		peers[int(k)] = v
	}
	if err := m.rc.refresh(peers); err != nil {
		return nil, err
	}

	return &mergepb.RefreshResponse{}, nil
}
