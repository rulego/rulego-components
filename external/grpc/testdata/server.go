/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	pb "github.com/rulego/rulego-components/external/grpc/testdata/helloworld"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

// server 是 Greeter 服务的服务器实现
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello 实现 Greeter 服务的 SayHello 方法
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	// 从上下文中获取元数据
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		// 打印所有头部信息
		for key, values := range md {
			log.Printf("Header %s: %v", key, values)
		}
	}
	log.Printf("Received: %v,%v", in.GetName(), in.GroupName)
	return &pb.HelloReply{Message: "Hello " + in.GetName(), GroupName: in.GroupName}, nil
}

func main() {
	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	// 注册 gRPC 反射服务
	reflection.Register(s)
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
