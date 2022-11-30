package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/v3/contrib/raftexample/mergepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"math/rand"
	"strconv"
	"time"
)

type merger struct {
	rc *raftNode
	ms *mergeStore

	mergeIdGen *idutil.Generator

	mergeProposeC chan<- proposal
	confChangeC   chan<- confChangeProposal
}

func newMerger(rc *raftNode, ms *mergeStore, kvPort uint32,
	mergeProposeC chan<- proposal, confChangeC chan<- confChangeProposal,
	mergeCommitC <-chan *commit, errorC <-chan error) *merger {
	m := &merger{rc: rc, ms: ms, mergeIdGen: idutil.NewGenerator(uint16(rc.id), time.Now()),
		mergeProposeC: mergeProposeC, confChangeC: confChangeC}
	go m.readCommit(kvPort, mergeCommitC, errorC)
	return m
}

func (m *merger) Propose(clusters []mergepb.Cluster) (uint64, error) {
	if clusters == nil || len(clusters) == 0 {
		return 0, fmt.Errorf("empty clusters")
	}

	if id, exists := m.ms.getOngoingId(); exists {
		return 0, fmt.Errorf("merge process with id %v is ongoning", id)
	}

	id := m.mergeIdGen.Next()
	msg := mergepb.MergeMessage{Id: id, Type: mergepb.MergeNew, Clusters: clusters}
	buf, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	wait := make(chan error)
	m.mergeProposeC <- proposal{Message: string(buf), wait: wait}
	if err := <-wait; err != nil {
		return 0, err
	}
	return id, err
}

func (m *merger) readCommit(kvPort uint32, mergeCommitC <-chan *commit, errorC <-chan error) {
	for ct := range mergeCommitC {
		for i := range ct.data {
			var msg mergepb.MergeMessage
			if err := msg.Unmarshal([]byte(ct.data[i])); err != nil {
				log.Panic(err)
			}

			switch msg.Type {
			case mergepb.MergeNew: // on coordinator cluster: start a new merge process
				m.handleNew(msg, kvPort)
			case mergepb.MergeSquashLog: // on coordinator cluster: apply transferred logs
				m.handleSquashLog(msg)
			case mergepb.MergeRefreshed: // on coordinator cluster: merge the refreshed node
				m.handleRefreshed(msg)
			case mergepb.MergeRedirect: // on candidate clusters: redirect future requests to coordinators
				url := fmt.Sprintf("http://%v:%v", msg.RedirectIp, msg.RedirectPort)
				log.Printf("Stop raft and redirect future requests to %v", url)
				redirect(url)
				m.rc.node.Stop()
			}
		}
		close(ct.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (m *merger) handleNew(msg mergepb.MergeMessage, kvPort uint32) {
	state := mergeState{
		Clusters:    msg.Clusters,
		NextIndexes: make([]uint64, len(msg.Clusters)),
		Merged:      make(map[uint64]bool),
	}
	for idx := range state.NextIndexes {
		// start from 1 since the first entry is a dummy one,
		// try to retrieve that from storage will cause error,
		// see commit 64d9bcabf1b for more
		state.NextIndexes[idx] = 1
	}
	for _, clr := range msg.Clusters {
		for id := range clr.Nodes {
			state.Merged[id] = false
		}
	}

	m.ms.setOngoingId(msg.Id)
	m.ms.put(msg.Id, state)
	m.ms.stats(msg.Id, "start", strconv.FormatInt(time.Now().UnixMilli(), 10))
	go m.startPullingLogs(kvPort, msg.Id, msg.Clusters, m.mergeProposeC)
}

func (m *merger) applySquashLog(msg mergepb.MergeMessage) {
	// convert logs into origin proposals and retrieve data
	data := make([]string, 0, len(msg.Logs))
	for i := range msg.Logs {
		var prop proposal
		if err := prop.unmarshall(msg.Logs[i]); err != nil {
			log.Fatal("apply logs from other Clusters failed!")
		}
		data = append(data, prop.Message)
	}

	// commit data to kv store
	log.Printf("Apply %v logs from cluster #%v", len(msg.Logs), msg.ClusterIdx)
	applyDoneC := make(chan struct{})
	m.rc.commitC <- &commit{data, applyDoneC}
	select {
	case <-applyDoneC:
	case <-m.rc.stopc:
		return
	}
}

func (m *merger) mergeSnapshot(msg mergepb.MergeMessage) {
	// commit data to kv store
	log.Printf("Apply snapshot before index %v on cluster #%v", msg.NextIdx, msg.ClusterIdx)
	applyDoneC := make(chan struct{})
	m.rc.commitC <- &commit{[]string{string(msg.Snapshot)}, applyDoneC}
	select {
	case <-applyDoneC:
	case <-m.rc.stopc:
		return
	}
}

func (m *merger) handleSquashLog(msg mergepb.MergeMessage) {
	mst, ok := m.ms.get(msg.Id, false)
	if !ok {
		panic(ok)
	}

	// expected next index from squash log
	nextIdx := mst.NextIndexes[msg.ClusterIdx]
	if nextIdx == 0 { // already finished
		return
	}

	if len(msg.Logs) != 0 {
		// different Nodes may propose the same squash log entry concurrently,
		// we only apply each squash log once
		if nextIdx >= msg.NextIdx {
			return
		}

		m.applySquashLog(msg)
	} else if len(msg.Snapshot) != 0 {
		if nextIdx >= msg.NextIdx {
			return
		}

		m.mergeSnapshot(msg)
	} // else: the candidate cluster has no data at all

	// update progress
	log.Printf("Update cluster #%v's progress from %v to %v", msg.ClusterIdx, nextIdx, msg.NextIdx)
	if msg.LastBatch {
		log.Printf("Pulled all %v logs from cluster #%v", msg.NextIdx, msg.ClusterIdx)
		mst.NextIndexes[msg.ClusterIdx] = 0
	} else {
		mst.NextIndexes[msg.ClusterIdx] = msg.NextIdx
	}
	m.ms.put(msg.Id, mst)

	// check if pulled all logs from all clusters,
	// if yes, start refreshing those nodes
	if msg.LastBatch && mst.stage() > Pulling {
		log.Printf("Pulled all logs from all Clusters")
		go m.startRefreshingNodes(msg.Id, mst.Clusters, m.mergeProposeC)
	}
}

func (m *merger) handleRefreshed(msg mergepb.MergeMessage) {
	mst, ok := m.ms.get(msg.Id, false)
	if !ok {
		panic(ok)
	}

	// different Nodes may propose the same refresh entry concurrently,
	// we only merge each refreshed node once
	if merged, ok := mst.Merged[msg.NodeId]; ok && merged {
		return
	}

	// construct raft url of the refreshed node
	var url string
	for _, clr := range mst.Clusters {
		for id, node := range clr.Nodes {
			if id == msg.NodeId {
				url = fmt.Sprintf("http://%v:%v", node.Ip, node.RaftPort)
				break
			}
		}
		if url != "" {
			break
		}
	}

	// add the refreshed node to cluster
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  msg.NodeId,
		Context: []byte(url),
	}
	m.rc.confState = *m.rc.node.ApplyConfChange(cc)
	m.rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})

	// update progress
	mst.Merged[msg.NodeId] = true
	m.ms.put(msg.Id, mst)

	if mst.stage() == COMPLETED {
		m.ms.stats(msg.Id, "finish", strconv.FormatInt(time.Now().UnixMilli(), 10))
	}
}

func (m *merger) startPullingLogs(kvPort uint32, mid uint64, clusters []mergepb.Cluster, mergeProposeC chan<- proposal) {
	// if a node replays the logs during recovery and restarts this merge process after completion,
	// we do a quorum read so the node will block until all logs are replayed,
	// and at that time, it will know the process has completed
	if mst, ok := m.ms.get(mid, true); !ok {
		panic(ok)
	} else if mst.stage() > Pulling {
		return
	}
	log.Printf("Start pulling logs")

	for idx, clr := range clusters {
		go func(idx uint64, cls map[uint64]mergepb.Node) {
			// create clients to candidate cluster's nodes
			clients := make([]*mergepb.MergeClient, 0, len(cls))
			for {
				for _, node := range cls {
					target := fmt.Sprintf("%v:%v", node.Ip, node.MergePort)
					conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						log.Printf("Connect to %v failed, will ignore this server", target)
						continue
					}

					cli := mergepb.NewMergeClient(conn)
					clients = append(clients, &cli)
				}
				if len(clients) != 0 {
					break
				}
				log.Printf("No available connection to cluster %v", idx)
				time.Sleep(time.Second)
			}

			log.Printf("Start poll logs from cluster #%v", idx)
			cli := *clients[rand.Intn(len(clients))]
			for {
				// get most up-to-date state
				mst, ok := m.ms.get(mid, true)
				if !ok {
					panic(ok)
				}
				if mst.stage() > Pulling {
					return
				}
				progress := mst.NextIndexes[idx]

				// only use leader to request logs to avoid some duplications
				nst := m.rc.node.Status()
				if nst.Lead == 0 || nst.Lead != nst.ID {
					time.Sleep(1 * time.Second)
					continue
				}

				// pull logs by RPC
				log.Printf("Request logs from cluster #%v for logs starting at %v", idx, progress)
				resp, err := cli.GetLogs(context.Background(),
					&mergepb.LogRequest{Id: mid, Index: progress, KvPort: kvPort})
				if err != nil {
					log.Printf("Request log failed: %v", err)
					cli = *clients[rand.Intn(len(clients))]
					continue
				}

				// create squash log proposal
				msg := mergepb.MergeMessage{
					Id:         mid,
					Type:       mergepb.MergeSquashLog,
					ClusterIdx: idx,
					LastBatch:  resp.LastBatch,
				}
				if len(resp.Logs) != 0 {
					msg.Logs = resp.Logs
					msg.NextIdx = progress + uint64(len(resp.Logs))
				} else if len(resp.Snapshot) != 0 {
					msg.Snapshot = resp.Snapshot
					msg.NextIdx = resp.NextIdx
				}

				buf, err := msg.Marshal()
				if err != nil {
					log.Printf("Marshal proposal message failed: %v", err)
					continue
				}

				// propose and wait on commit
				log.Printf("Propose logs from cluster #%v for logs starting at %v", idx, mst.NextIndexes[idx])
				wc := make(chan error)
				mergeProposeC <- proposal{Message: string(buf), wait: wc}
				select {
				case err = <-wc:
					if err != nil {
						log.Printf("Propose logs from cluster #%v for logs starting at %v failed: %v",
							idx, progress, err)
						continue
					}
				case <-m.rc.stopc:
					return
				}

				if resp.LastBatch {
					break
				}
			}
		}(uint64(idx), clr.Nodes)
	}
}

func (m *merger) startRefreshingNodes(mid uint64, clusters []mergepb.Cluster, mergeProposeC chan<- proposal) {
	// if a node replays the logs during recovery and restarts this merge process after completion,
	// we do a quorum read so the node will block until all logs are replayed,
	// and at that time, it will know the process has completed
	if mst, ok := m.ms.get(mid, true); !ok {
		panic(ok)
	} else if mst.stage() > Refreshing {
		return
	}

	candidates := make(map[uint64]mergepb.Node)
	for _, clr := range clusters {
		for id, node := range clr.Nodes {
			candidates[id] = node
		}
	}

	log.Printf("Start refreshing and adding nodes")
	for id, node := range candidates {
		go func(id uint64, node mergepb.Node) {
			// create client to the candidate node
			var client mergepb.MergeClient
			for {
				url := fmt.Sprintf("%v:%v", node.Ip, node.MergePort)
				conn, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("Connect to %v failed: %v", url, err)
					time.Sleep(time.Second)
					continue
				}
				client = mergepb.NewMergeClient(conn)
				break
			}

			// refresh the node by RPC
			for {
				// get most up-to-date state
				mst, ok := m.ms.get(mid, true)
				if !ok {
					panic(ok)
				}
				if mst.Merged[id] {
					return
				}

				status := m.rc.node.Status()
				if status.Lead == 0 || status.Lead != status.ID {
					time.Sleep(1 * time.Second)
					continue
				}

				peers := make(map[uint64]string)
				for k, v := range m.rc.peers {
					peers[uint64(k)] = v
				}
				peers[id] = fmt.Sprintf("http://%v:%v", node.Ip, node.RaftPort)

				if _, err := client.Refresh(context.Background(),
					&mergepb.RefreshRequest{RequesterId: uint64(m.rc.id), Peers: peers}); err != nil {
					log.Printf("Refresh node %v failed: %v", id, err)
					continue
				}
				log.Printf("Refreshed node %v", id)
				break
			}

			// create refreshed proposal
			for {
				// get most up-to-date state
				mst, ok := m.ms.get(mid, true)
				if !ok {
					panic(ok)
				}
				if mst.Merged[id] {
					return
				}

				msg := mergepb.MergeMessage{
					Id:     mid,
					Type:   mergepb.MergeRefreshed,
					NodeId: id,
				}
				buf, err := msg.Marshal()
				if err != nil {
					log.Printf("Marshal proposal message failed: %v", err)
					continue
				}

				log.Printf("Propose refreshed message for node %v", id)
				wc := make(chan error)
				mergeProposeC <- proposal{Message: string(buf), wait: wc}
				select {
				case err = <-wc:
					if err != nil {
						log.Printf("Propose refreshed message for node %v failed: %v", id, err)
						continue
					}
				case <-m.rc.stopc:
					return
				}
				break
			}
		}(id, node)
	}
}
