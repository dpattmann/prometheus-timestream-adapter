package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/prometheus/prometheus/prompb"
	"reflect"
	"testing"
)

func Test_readLabels(t *testing.T) {
	type args struct {
		labels []*prompb.Label
	}
	tests := []struct {
		name            string
		args            args
		wantDimensions  []*timestreamwrite.Dimension
		wantMeasureName string
	}{
		{
			name: "Prom data request",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "sample_metric",
					},
					{
						Name:  "job",
						Value: "testing",
					},
				},
			},
			wantDimensions: []*timestreamwrite.Dimension{
				{
					Name:  aws.String("job"),
					Value: aws.String("testing"),
				},
			},
			wantMeasureName: "sample_metric",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDimensions, gotMeasureName := readLabels(tt.args.labels)
			if !reflect.DeepEqual(gotDimensions, tt.wantDimensions) {
				t.Errorf("readLabels() gotDimensions = %v, want %v", gotDimensions, tt.wantDimensions)
			}
			if gotMeasureName != tt.wantMeasureName {
				t.Errorf("readLabels() gotMeasureName = %v, want %v", gotMeasureName, tt.wantMeasureName)
			}
		})
	}
}

func Test_protoToRecords(t *testing.T) {
	type args struct {
		req *prompb.WriteRequest
	}
	tests := []struct {
		name        string
		args        args
		wantRecords []*timestreamwrite.Record
	}{
		{
			name: "Prom data request",
			args: args{
				req: &prompb.WriteRequest{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []*prompb.Label{
								{
									Name:  "__name__",
									Value: "sample_metric",
								},
								{
									Name:  "job",
									Value: "testing",
								},
							},
							Samples: []prompb.Sample{
								{
									Value:     float64(12345),
									Timestamp: int64(1604170583682),
								},
							},
						},
					},
				},
			},
			wantRecords: []*timestreamwrite.Record{
				{
					Dimensions: []*timestreamwrite.Dimension{
						{
							Name:  aws.String("job"),
							Value: aws.String("testing"),
						},
					},
					MeasureName:      aws.String("sample_metric"),
					MeasureValue:     aws.String("12345"),
					MeasureValueType: aws.String("DOUBLE"),
					Time:             aws.String("1604170583.682"),
					TimeUnit:         aws.String("MILLISECONDS"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRecords := protoToRecords(tt.args.req); !reflect.DeepEqual(gotRecords, tt.wantRecords) {
				t.Errorf("protoToRecords() = %v, want %v", gotRecords, tt.wantRecords)
			}
		})
	}
}
