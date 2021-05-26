module go.etcd.io/etcd/v3

go 1.16

replace (
	go.etcd.io/etcd/api/v3 => ./api
	go.etcd.io/etcd/client/pkg/v3 => ./client/pkg
	go.etcd.io/etcd/client/v2 => ./client/v2
	go.etcd.io/etcd/client/v3 => ./client/v3
	go.etcd.io/etcd/etcdctl/v3 => ./etcdctl
	go.etcd.io/etcd/etcdutl/v3 => ./etcdutl
	go.etcd.io/etcd/pkg/v3 => ./pkg
	go.etcd.io/etcd/raft/v3 => ./raft
	go.etcd.io/etcd/server/v3 => ./server
	go.etcd.io/etcd/tests/v3 => ./tests
)

require (
	github.com/bgentry/speakeasy v0.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/spf13/cobra v1.1.3
	go.etcd.io/bbolt v1.3.6-0.20210426205525-9c92be978ae0
	go.etcd.io/etcd/api/v3 v3.5.0-beta.4
	go.etcd.io/etcd/client/pkg/v3 v3.5.0-beta.4
	go.etcd.io/etcd/client/v2 v2.305.0-beta.4
	go.etcd.io/etcd/client/v3 v3.5.0-beta.4
	go.etcd.io/etcd/etcdctl/v3 v3.5.0-beta.4
	go.etcd.io/etcd/etcdutl/v3 v3.5.0-beta.4
	go.etcd.io/etcd/pkg/v3 v3.5.0-beta.4
	go.etcd.io/etcd/raft/v3 v3.5.0-beta.4
	go.etcd.io/etcd/server/v3 v3.5.0-beta.4
	go.etcd.io/etcd/tests/v3 v3.5.0-beta.4
	go.uber.org/zap v1.16.1-0.20210329175301-c23abee72d19
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.37.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28
)
