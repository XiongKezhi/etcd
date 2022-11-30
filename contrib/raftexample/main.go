// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
)

func main() {
	cluster := flag.String("cluster", "1=http://127.0.0.1:9021", "comma separated cluster peers, each in format id=url")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	mergePort := flag.Int("mergeport", 9122, "merge server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	peers := make(map[int]string)
	for _, p := range strings.Split(*cluster, ",") {
		pinfo := strings.Split(p, "=")
		pid, err := strconv.Atoi(pinfo[0])
		if err != nil {
			log.Fatal(fmt.Sprintf("parse peer %v failed: %v", p, err))
		}
		peers[pid] = pinfo[1]
	}

	proposeC := make(chan proposal)
	defer close(proposeC)
	mergeProposeC := make(chan proposal)
	defer close(mergeProposeC)
	confChangeC := make(chan confChangeProposal)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	rc, commitC, mergeCommitC, errorC, snapshotterReady :=
		newRaftNode(*id, peers, *join, getSnapshot, proposeC, mergeProposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	mr := newMerger(rc, NewMergeStore(kvs, proposeC), uint32(*kvport),
		mergeProposeC, confChangeC, mergeCommitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, mr, *kvport, confChangeC)

	// the merge rpc server will server merge related calls
	serveRpcMergeAPI(rc, *mergePort, mergeProposeC)

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
