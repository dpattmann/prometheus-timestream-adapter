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
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamquery/timestreamqueryiface"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/aws/aws-sdk-go/service/timestreamwrite/timestreamwriteiface"

	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

var (
	timeStreamAdapter = &TimeStreamAdapter{
		TimestreamQueryAPI: TimeStreamQueryMock{},
		TimestreamWriteAPI: TimeStreamWriterMock{},
		databaseName:       "mockDatabase",
		logger:             zap.NewNop().Sugar(),
		tableName:          "mockTable",
	}

	measureOutput = &timestreamquery.QueryOutput{
		ColumnInfo: []*timestreamquery.ColumnInfo{
			{
				Name: aws.String("measure_name"),
				Type: &timestreamquery.Type{ScalarType: aws.String("VARCHAR")},
			},
			{
				Name: aws.String("data_type"),
				Type: &timestreamquery.Type{ScalarType: aws.String("VARCHAR")},
			},
			{
				Name: aws.String("dimensions"),
				Type: &timestreamquery.Type{
					ArrayColumnInfo: &timestreamquery.ColumnInfo{
						Type: &timestreamquery.Type{
							RowColumnInfo: []*timestreamquery.ColumnInfo{
								{
									Name: aws.String("dimension_name"),
									Type: &timestreamquery.Type{ScalarType: aws.String("VARCHAR")},
								},
								{
									Name: aws.String("data_type"),
									Type: &timestreamquery.Type{ScalarType: aws.String("VARCHAR")},
								},
							},
						},
					},
				},
			},
		},
		QueryId: aws.String("MOCK"),
		Rows: []*timestreamquery.Row{
			{
				Data: []*timestreamquery.Datum{
					{ScalarValue: aws.String("mock")},
					{ScalarValue: aws.String("double")},
					{
						ArrayValue: []*timestreamquery.Datum{
							{
								RowValue: &timestreamquery.Row{
									Data: []*timestreamquery.Datum{
										{ScalarValue: aws.String("instance")},
										{ScalarValue: aws.String("varchar")},
									},
								},
							},
							{
								RowValue: &timestreamquery.Row{
									Data: []*timestreamquery.Datum{
										{ScalarValue: aws.String("job")},
										{ScalarValue: aws.String("varchar")},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	queryOutputColumns = []*timestreamquery.ColumnInfo{
		{
			Name: aws.String("instance"),
			Type: &timestreamquery.Type{ScalarType: aws.String("VARCHAR")},
		},
		{
			Name: aws.String("job"),
			Type: &timestreamquery.Type{ScalarType: aws.String("VARCHAR")},
		},
		{
			Name: aws.String("mock"),
			Type: &timestreamquery.Type{
				TimeSeriesMeasureValueColumnInfo: &timestreamquery.ColumnInfo{
					Type: &timestreamquery.Type{ScalarType: aws.String("DOUBLE")},
				},
			},
		},
	}

	queryOutput0 = &timestreamquery.QueryOutput{
		ColumnInfo: queryOutputColumns,
		QueryId:    aws.String("MOCK"),
	}

	queryOutput1 = &timestreamquery.QueryOutput{
		ColumnInfo: queryOutputColumns,
		QueryId:    aws.String("MOCK"),
		Rows: []*timestreamquery.Row{
			{
				Data: []*timestreamquery.Datum{
					{ScalarValue: aws.String("host:9100")},
					{ScalarValue: aws.String("mock-exporter")},
					{
						TimeSeriesValue: []*timestreamquery.TimeSeriesDataPoint{
							{
								Time:  aws.String("2020-01-01 00:00:00.000000000"),
								Value: &timestreamquery.Datum{ScalarValue: aws.String("1.0")},
							},
						},
					},
				},
			},
		},
	}

	queryOutput2 = &timestreamquery.QueryOutput{
		ColumnInfo: queryOutputColumns,
		QueryId:    aws.String("MOCK"),
		Rows: []*timestreamquery.Row{
			{
				Data: []*timestreamquery.Datum{
					{ScalarValue: aws.String("host:9100")},
					{ScalarValue: aws.String("mock-exporter")},
					{
						TimeSeriesValue: []*timestreamquery.TimeSeriesDataPoint{
							{
								Time:  aws.String("2020-01-01 00:00:01.000000000"),
								Value: &timestreamquery.Datum{ScalarValue: aws.String("2.0")},
							},
						},
					},
				},
			},
		},
	}
)

type TimeStreamWriterMock struct {
	timestreamwriteiface.TimestreamWriteAPI
}

type TimeStreamQueryMock struct {
	timestreamqueryiface.TimestreamQueryAPI
}

func (t TimeStreamWriterMock) WriteRecords(input *timestreamwrite.WriteRecordsInput) (*timestreamwrite.WriteRecordsOutput, error) {
	for _, i := range input.Records {
		if *i.MeasureName == "sample_name_error" {
			return nil, errors.New("error writing to mock timestream database")
		}
	}
	return &timestreamwrite.WriteRecordsOutput{}, nil
}

func (t TimeStreamQueryMock) Query(input *timestreamquery.QueryInput) (output *timestreamquery.QueryOutput, err error) {
	switch *input.QueryString {
	case "SHOW MEASURES FROM \"prometheus-database\".\"prometheus-table\" LIKE 'mock'":
		output = measureOutput
	case "SELECT instance, job, CREATE_TIME_SERIES(time, measure_value::double) AS mock FROM \"prometheus-database\".\"prometheus-table\" WHERE measure_name = 'mock' AND time BETWEEN from_milliseconds(1577836800000) AND from_milliseconds(1577836800000) GROUP BY instance, job":
		output = queryOutput1
	}

	return
}

func (t TimeStreamQueryMock) QueryPages(input *timestreamquery.QueryInput, handler func(*timestreamquery.QueryOutput, bool) bool) (err error) {
	handler(queryOutput0, false)
	handler(queryOutput1, false)
	handler(queryOutput2, true)
	return
}

func TestTimeSteamAdapter_readLabels(t *testing.T) {
	type args struct {
		labels []prompb.Label
	}
	tests := []struct {
		name     string
		args     args
		wantTask writeTask
	}{
		{
			name: "Prom data request",
			args: args{
				labels: []prompb.Label{
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
			wantTask: writeTask{
				measureName: "sample_metric",
				dimensions: []*timestreamwrite.Dimension{
					{
						Name:  aws.String("job"),
						Value: aws.String("testing"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotTask := timeStreamAdapter.readLabels(tt.args.labels); !reflect.DeepEqual(gotTask, tt.wantTask) {
				t.Errorf("readLabels() = %v, want %v", gotTask, tt.wantTask)
			}
		})
	}
}

func TestTimeSteamAdapter_toRecords(t *testing.T) {
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
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
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
									Timestamp: int64(1577836800000),
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
					Time:             aws.String("1577836800000"),
					TimeUnit:         aws.String("MILLISECONDS"),
				},
			},
		},
		{
			name: "Prom with NaN value",
			args: args{
				req: &prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
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
									Timestamp: int64(1577836800000),
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
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
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
									Timestamp: int64(1577836800000),
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
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
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
									Timestamp: int64(1577836800000),
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
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
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
									Timestamp: int64(1577836800000),
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
			if gotRecords := timeStreamAdapter.toRecords(tt.args.req); !reflect.DeepEqual(gotRecords, tt.wantRecords) {
				t.Errorf("toRecords() = %v, want %v", gotRecords, tt.wantRecords)
			}
		})
	}
}

func TestTimeSteamAdapter_Write(t *testing.T) {
	type args struct {
		req *prompb.WriteRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Write Timestream Request",
			args: args{
				req: &prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{
									Name:  "__name__",
									Value: "sample_name",
								},
							},
							Samples: []prompb.Sample{
								{
									Value:     float64(123456),
									Timestamp: time.Now().Unix(),
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Write Timestream Request With Error",
			args: args{
				req: &prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{
									Name:  "__name__",
									Value: "sample_name_error",
								},
							},
							Samples: []prompb.Sample{
								{
									Value:     float64(123456),
									Timestamp: time.Now().Unix(),
								},
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := timeStreamAdapter.Write(tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTimeSteamAdapter_Read(t *testing.T) {
	type args struct {
		request *prompb.ReadRequest
	}
	tests := []struct {
		name         string
		args         args
		wantResponse *prompb.ReadResponse
		wantErr      bool
	}{
		{
			name: "Read Timestream Request",
			args: args{
				request: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							StartTimestampMs: int64(1577836800000),
							EndTimestampMs:   int64(1577836800000),
							Matchers: []*prompb.LabelMatcher{
								{
									Name:  "__name__",
									Value: "mock",
								},
							},
							Hints: &prompb.ReadHints{
								StartMs: 1607529098049,
								EndMs:   1607529398049,
							},
						},
					},
				},
			},
			wantResponse: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{
									{
										Name:  "__name__",
										Value: "mock",
									},
									{
										Name:  "instance",
										Value: "host:9100",
									},
									{
										Name:  "job",
										Value: "mock-exporter",
									},
								},
								Samples: []prompb.Sample{
									{
										Value:     1.0,
										Timestamp: 1577836800000,
									},
								},
							},
							{
								Labels: []prompb.Label{
									{
										Name:  "__name__",
										Value: "mock",
									},
									{
										Name:  "instance",
										Value: "host:9100",
									},
									{
										Name:  "job",
										Value: "mock-exporter",
									},
								},
								Samples: []prompb.Sample{
									{
										Value:     2.0,
										Timestamp: 1577836801000,
									},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := timeStreamAdapter.Read(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResponse, tt.wantResponse) {
				t.Errorf("Read() gotResponse = %v, want %v", gotResponse, tt.wantResponse)
			}
		})
	}
}
