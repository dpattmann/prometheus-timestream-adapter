module github.com/dpattmann/prometheus-timestream-adapter

go 1.15

require (
	github.com/aws/aws-sdk-go v1.40.58
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.31.1
	github.com/prometheus/prometheus v2.5.0+incompatible
	github.com/spf13/pflag v1.0.5
	go.uber.org/zap v1.19.1
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e
	google.golang.org/genproto v0.0.0-20201030142918-24207fddd1c3 // indirect
)
