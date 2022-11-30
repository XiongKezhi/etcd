package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"go.etcd.io/etcd/v3/contrib/raftexample/mergepb"
	"strconv"
)

type mergeStage int

const (
	NONE mergeStage = iota
	Pulling
	Refreshing
	COMPLETED
)

func (ms mergeStage) String() string {
	switch ms {
	case NONE:
		return "none"
	case Pulling:
		return "pulling"
	case Refreshing:
		return "refreshing"
	case COMPLETED:
		return "completed"
	default:
		return "unknown"
	}
}

type mergeState struct {
	Clusters    []mergepb.Cluster
	NextIndexes []uint64        // progress for log pull stage
	Merged      map[uint64]bool // progress for refresh stage
}

func (s *mergeState) stage() mergeStage {
	cnt := 0
	for i := range s.NextIndexes {
		if s.NextIndexes[i] == 0 {
			cnt++
		}
	}
	if cnt < len(s.Clusters) {
		return Pulling
	}

	cnt = 0
	for _, refreshed := range s.Merged {
		if refreshed {
			cnt++
		}
	}
	if cnt < len(s.Merged) {
		return Refreshing
	}

	return COMPLETED
}

func (s *mergeState) marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *mergeState) unmarshal(data []byte) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(s)
}

// special keys in kvstore to store metadata of merge
const (
	MergePrefix    = "/merge"
	MergeOngoingId = MergePrefix + "/ongoing"
	MergeNop       = MergePrefix + "/nop"
	MergeStats     = MergePrefix + "/stats"
)

func key(id uint64) string {
	return fmt.Sprintf("%v/%v", MergePrefix, id)
}

type mergeStore struct {
	proposeC chan<- proposal
	store    *kvstore
}

func NewMergeStore(kvs *kvstore, proposeC chan<- proposal) *mergeStore {
	return &mergeStore{proposeC: proposeC, store: kvs}
}

func (s *mergeStore) mustPropose(k string, v string) {
	for {
		if s.store.Propose(k, v, true) == nil {
			return
		}
	}
}

func (s *mergeStore) setOngoingId(id uint64) {
	s.store.Put(MergeOngoingId, strconv.FormatUint(id, 10))
}

func (s *mergeStore) getOngoingId() (uint64, bool) {
	s.mustPropose(MergeNop, MergeNop)
	v, ok := s.store.Lookup(MergeOngoingId)
	if !ok {
		return 0, false
	}

	id, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		panic(err)
	}

	mst, ok := s.get(id, false)
	if !ok {
		panic(err)
	}
	if mst.stage() == COMPLETED {
		return 0, false
	}

	return id, true
}

func (s *mergeStore) put(id uint64, state mergeState) {
	data, err := state.marshal()
	if err != nil {
		panic(err)
	}
	s.store.Put(key(id), string(data))
}

func (s *mergeStore) get(id uint64, quorum bool) (mergeState, bool) {
	if quorum {
		s.mustPropose(MergeNop, MergeNop)
	}

	v, ok := s.store.Lookup(key(id))
	if !ok {
		return mergeState{}, false
	}

	mst := mergeState{}
	if err := mst.unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	return mst, true
}

func (s *mergeStore) stats(id uint64, subkey string, value string) {
	s.store.Put(fmt.Sprintf("%s/%s/%s", MergeStats, strconv.FormatUint(id, 10), subkey), value)
}
