// 服务端实现
package main

import (
	"fmt"
	pb "main/api/ble/v1"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dataServer struct {
	pb.UnimplementedDataServiceServer
}

func (s *dataServer) StreamData(req *pb.StreamRequest, stream pb.DataService_StreamDataServer) error {
	// 模拟发送不同类型的数据
	for {
		// 发送温度数据
		tempData := &pb.DataResponse{
			Type:      "temperature",
			Payload:   []byte(`{"value": 25.5, "unit": "C"}`),
			Timestamp: time.Now().Unix(),
		}
		if err := stream.Send(tempData); err != nil {
			return err
		}

		time.Sleep(time.Second)

		// 发送湿度数据
		humidityData := &pb.DataResponse{
			Type:      "humidity",
			Payload:   []byte(`{"value": 60, "unit": "%"}`),
			Timestamp: time.Now().Unix(),
		}
		if err := stream.Send(humidityData); err != nil {
			return err
		}

		time.Sleep(time.Second)
	}
}

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDataServiceServer(grpcServer, &dataServer{})

	// 注册反射服务 - 添加这行
	reflection.Register(grpcServer)

	fmt.Println("Server starting at :9000")
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}
