all: message.pb.go

message.pb.go: message.proto
	protoc --go_out=. message.proto
