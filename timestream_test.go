// Copyright 2020 Dennis Pattmann <d.pattmann@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// @author		Dennis Pattmann <d.pattmann@gmail.com>
// @copyright 	2020 Dennis Pattmann <d.pattmann@gmail.com>
// @license 	Apache-2.0

package main

import (
	"math"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

var (
	mockAdapter = newTimeStreamAdapter(zap.NewNop().Sugar(), cfg, TimeStreamWriterMock{}, TimeStreamQueryMock{})
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
			gotDimensions, gotMeasureName := mockAdapter.readLabels(tt.args.labels)
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
									Timestamp: int64(1604254700024),
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
					Time:             aws.String("1604254700024"),
					TimeUnit:         aws.String("MILLISECONDS"),
				},
			},
		},
		{
			name: "Prom with NaN value",
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
									Value:     math.NaN(),
									Timestamp: int64(1604254700024),
								},
							},
						},
					},
				},
			},
			wantRecords: nil,
		},
		{
			name: "Prom with positive inf number",
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
									Value:     math.Inf(1),
									Timestamp: int64(1604254700024),
								},
							},
						},
					},
				},
			},
			wantRecords: nil,
		},
		{
			name: "Prom with negative inf number",
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
									Value:     math.Inf(-1),
									Timestamp: int64(1604254700024),
								},
							},
						},
					},
				},
			},
			wantRecords: nil,
		},
		{
			name: "Prom with long metric name",
			args: args{
				req: &prompb.WriteRequest{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []*prompb.Label{
								{
									Name:  "__name__",
									Value: "sample_metric_measure_name_exceeds_the_maximum_supported_length",
								},
								{
									Name:  "job",
									Value: "testing",
								},
							},
							Samples: []prompb.Sample{
								{
									Value:     float64(12345),
									Timestamp: int64(1604254700024),
								},
							},
						},
					},
				},
			},
			wantRecords: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRecords := mockAdapter.protoToRecords(tt.args.req); !reflect.DeepEqual(gotRecords, tt.wantRecords) {
				t.Errorf("protoToRecords() = %v, want %v", gotRecords, tt.wantRecords)
			}
		})
	}
}
