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
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/v3/contrib/raftexample/mergepb"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore
	mr          *merger
	confChangeC chan<- confChangeProposal
}

const waitPrefix = "/wait"

type Node struct {
	Ip        string `json:"ip"`
	MergePort uint32 `json:"mergePort"`
	RaftPort  uint32 `json:"raftPort"`
}

type mergeRequest struct {
	Clusters []map[string]Node `json:"clusters"`
}

type mergeResponse struct {
	Id uint64 `json:"id"`
}

var (
	redirectUrl = ""
	redirectLok = sync.RWMutex{}
)

func redirect(url string) {
	redirectLok.Lock()
	defer redirectLok.Unlock()
	redirectUrl = url
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	redirectLok.RLock()
	if redirectUrl != "" {
		http.Error(w, "retry on "+redirectUrl, http.StatusServiceUnavailable)
		redirectLok.RUnlock()
		return
	} else {
		redirectLok.RUnlock()
	}

	key := r.RequestURI
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		wait := false
		if strings.HasPrefix(key, waitPrefix+"/") {
			key = key[len(waitPrefix):]
			wait = true
		}

		if err := h.store.Propose(key, string(v), wait); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(err.Error()))
			return
		}

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if strings.HasPrefix(key, "/merge") && !strings.HasPrefix(key, "/merge/stats") {
			key = strings.TrimPrefix(key, "/merge/")
			id, err := strconv.ParseUint(key, 10, 64)
			if err != nil {
				log.Printf("Failed to parse merge id: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			mst, ok := h.mr.ms.get(id, false)
			if ok {
				w.Write([]byte(mst.stage().String()))
			} else {
				http.Error(w, "Unknown merge id: "+key, http.StatusNotFound)
			}
			return
		}

		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		if strings.HasPrefix(key, "/merge") {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Failed to read on POST (%v)\n", err)
				http.Error(w, fmt.Sprintf("Failed on POST: %v", err), http.StatusBadRequest)
				return
			}

			req := mergeRequest{}
			if err := json.Unmarshal(body, &req); err != nil {
				log.Printf("Failed to parse body: %v\n", err)
				http.Error(w, fmt.Sprintf("Failed on POST: %v", err), http.StatusBadRequest)
				return
			}

			clusters := make([]mergepb.Cluster, 0, len(req.Clusters))
			for _, clr := range req.Clusters {
				if len(clr) == 0 {
					http.Error(w, fmt.Sprintf("Failed on POST: Empty cluster"), http.StatusBadRequest)
					return
				}

				cluster := mergepb.Cluster{Nodes: make(map[uint64]mergepb.Node)}
				for idStr, node := range clr {
					id, err := strconv.ParseUint(idStr, 10, 64)
					if err != nil {
						log.Printf("Failed to parse id: %v\n", err)
						http.Error(w, fmt.Sprintf("Failed on POST: %v", err), http.StatusBadRequest)
						return
					}
					cluster.Nodes[id] = mergepb.Node{Ip: node.Ip, MergePort: node.MergePort, RaftPort: node.RaftPort}
				}
				clusters = append(clusters, cluster)
			}

			id, err := h.mr.Propose(clusters)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(err.Error()))
				return
			}

			data, err := json.Marshal(mergeResponse{Id: id})
			if err != nil {
				log.Printf("Failed to marshal response: %v\n", err)
				http.Error(w, fmt.Sprintf("Failed on POST: %v", err), http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}

		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		h.confChangeC <- confChangeProposal{cc: cc, wait: nil}

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeC <- confChangeProposal{cc: cc, wait: nil}

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(kv *kvstore, mr *merger, port int, confChangeC chan<- confChangeProposal) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			mr:          mr,
			confChangeC: confChangeC,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}
