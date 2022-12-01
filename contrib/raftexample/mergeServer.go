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
	ms            *mergeStore
	refreshKv     func()
	mergeProposeC chan<- proposal
	mergepb.UnimplementedMergeServer
}

// serveRpcMergeAPI starts a merge server to server RPC calls.
func serveRpcMergeAPI(rc *raftNode, ms *mergeStore, refreshKv func(), mergePort int, mergeProposeC chan<- proposal) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", mergePort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	mergepb.RegisterMergeServer(srv, &mergeServer{rc: rc, ms: ms, refreshKv: refreshKv, mergeProposeC: mergeProposeC})
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

const batchSize = 128                // number of logs in one batch
const maxSize = 18446744073709551615 // max size in bytes

func (m *mergeServer) Try(ctx context.Context, in *mergepb.TryRequest) (*mergepb.TryResponse, error) {
	log.Printf("Received request to start merge with id %v", in.Id)

	// check if already has an ongoing one
	if id, ok := m.ms.getOngoingId(true); ok {
		log.Printf("Try new merge failed due to ongoing merge with id %v", in.Id)
		return &mergepb.TryResponse{Id: id}, nil
	}

	// check if request cluster information is the same to the cluster config
	if len(in.Cluster.Nodes) != len(m.rc.confState.Voters) {
		log.Printf("Cluster configuration not match")
		return &mergepb.TryResponse{}, fmt.Errorf("cluster configuration not match")
	}
	conf := make(map[uint64]struct{})
	for _, vid := range m.rc.confState.Voters {
		conf[vid] = struct{}{}
	}
	for _, node := range in.Cluster.Nodes {
		if _, ok := conf[node.Id]; !ok {
			log.Printf("Cluster configuration not match")
			return &mergepb.TryResponse{}, fmt.Errorf("cluster configuration not match")
		}
	}

	// try a new one
	msg := mergepb.MergeMessage{Id: in.Id, Type: mergepb.MergeTry}
	msgBytes, err := msg.Marshal()
	if err != nil {
		return &mergepb.TryResponse{}, err
	}

	wc := make(chan error)
	m.mergeProposeC <- proposal{Message: string(msgBytes), wait: wc}
	if err := <-wc; err != nil && !errors.Is(err, raft.ErrStopped) {
		log.Printf("Try new merge failed: %v", err)
		return &mergepb.TryResponse{}, fmt.Errorf("try ner merge error: %v", err)
	}

	// retrieve ongoing id
	id, ok := m.ms.getOngoingId(true)
	if !ok {
		return &mergepb.TryResponse{Id: id}, fmt.Errorf("no merge id successfully tried")
	}

	log.Printf("Try cluster succeed for merge with id %v", id)
	return &mergepb.TryResponse{Id: id}, nil
}

func (m *mergeServer) GetLogs(ctx context.Context, in *mergepb.LogRequest) (*mergepb.LogResponse, error) {
	log.Printf("Received request for log starting from %v", in.Index)

	// get all following logs, shouldn't be performance problem since the method returns a slice (a reference)
	ents, err := m.rc.raftStorage.Entries(in.Index, m.rc.node.Status().Commit+1, maxSize)
	if err != nil {
		// if requested logs are compacted, send a snapshot of application data
		if err == raft.ErrCompacted {
			nextIdx, err := m.rc.raftStorage.FirstIndex()
			if err != nil {
				log.Printf("Fetch first index failed: %v", err)
			}
			log.Printf("Send snapshot for compacted logs before %v", nextIdx)

			snapshot, err := m.rc.getSnapshot()
			if err != nil {
				log.Printf("Fetch snapshot failed: %v", err)
			}

			return &mergepb.LogResponse{Snapshot: snapshot, NextIdx: nextIdx, LastBatch: false}, nil
		}

		log.Printf("Fetch entries failed: %v", err)
		return &mergepb.LogResponse{}, err
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
			log.Printf("Responded with %v log entries", len(data))
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
		return &mergepb.LogResponse{}, err
	}

	// propose and wait on commit and apply
	wc := make(chan error)
	m.mergeProposeC <- proposal{Message: string(msgBytes), wait: wc}
	if err := <-wc; err != nil && !errors.Is(err, raft.ErrStopped) {
		log.Printf("Propose last batch failed: %v", err)
		return &mergepb.LogResponse{}, fmt.Errorf("propose last batch error: %v", err)
	}

	log.Printf("Responded with %v log entries in the last batch", len(data))
	return &mergepb.LogResponse{Logs: data, LastBatch: true}, nil
}

func (m *mergeServer) Refresh(ctx context.Context, in *mergepb.RefreshRequest) (*mergepb.RefreshResponse, error) {
	log.Printf("Received request from node #%v to refresh node", in.RequesterId)

	peers := make(map[int]string)
	for k, v := range in.Peers {
		peers[int(k)] = v
	}
	if err := m.rc.refresh(peers); err != nil {
		return &mergepb.RefreshResponse{}, err
	}

	m.refreshKv()
	return &mergepb.RefreshResponse{}, nil
}
