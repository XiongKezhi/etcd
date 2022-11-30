// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.17.3
// source: merge.proto

package mergepb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// MergeClient is the client API for Merge service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MergeClient interface {
	GetLogs(ctx context.Context, in *LogRequest, opts ...grpc.CallOption) (*LogResponse, error)
	Refresh(ctx context.Context, in *RefreshRequest, opts ...grpc.CallOption) (*RefreshResponse, error)
}

type mergeClient struct {
	cc grpc.ClientConnInterface
}

func NewMergeClient(cc grpc.ClientConnInterface) MergeClient {
	return &mergeClient{cc}
}

func (c *mergeClient) GetLogs(ctx context.Context, in *LogRequest, opts ...grpc.CallOption) (*LogResponse, error) {
	out := new(LogResponse)
	err := c.cc.Invoke(ctx, "/mergepb.Merge/GetLogs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mergeClient) Refresh(ctx context.Context, in *RefreshRequest, opts ...grpc.CallOption) (*RefreshResponse, error) {
	out := new(RefreshResponse)
	err := c.cc.Invoke(ctx, "/mergepb.Merge/Refresh", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MergeServer is the server API for Merge service.
// All implementations must embed UnimplementedMergeServer
// for forward compatibility
type MergeServer interface {
	GetLogs(context.Context, *LogRequest) (*LogResponse, error)
	Refresh(context.Context, *RefreshRequest) (*RefreshResponse, error)
	mustEmbedUnimplementedMergeServer()
}

// UnimplementedMergeServer must be embedded to have forward compatible implementations.
type UnimplementedMergeServer struct {
}

func (UnimplementedMergeServer) GetLogs(context.Context, *LogRequest) (*LogResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLogs not implemented")
}
func (UnimplementedMergeServer) Refresh(context.Context, *RefreshRequest) (*RefreshResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Refresh not implemented")
}
func (UnimplementedMergeServer) mustEmbedUnimplementedMergeServer() {}

// UnsafeMergeServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MergeServer will
// result in compilation errors.
type UnsafeMergeServer interface {
	mustEmbedUnimplementedMergeServer()
}

func RegisterMergeServer(s grpc.ServiceRegistrar, srv MergeServer) {
	s.RegisterService(&Merge_ServiceDesc, srv)
}

func _Merge_GetLogs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LogRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MergeServer).GetLogs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/mergepb.Merge/GetLogs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MergeServer).GetLogs(ctx, req.(*LogRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Merge_Refresh_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RefreshRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MergeServer).Refresh(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/mergepb.Merge/Refresh",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MergeServer).Refresh(ctx, req.(*RefreshRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Merge_ServiceDesc is the grpc.ServiceDesc for Merge service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Merge_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "mergepb.Merge",
	HandlerType: (*MergeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetLogs",
			Handler:    _Merge_GetLogs_Handler,
		},
		{
			MethodName: "Refresh",
			Handler:    _Merge_Refresh_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "merge.proto",
}
