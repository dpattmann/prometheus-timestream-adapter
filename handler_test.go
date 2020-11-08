package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/aws/aws-sdk-go/service/timestreamwrite/timestreamwriteiface"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type TimeStreamWriterMock struct {
	timestreamwriteiface.TimestreamWriteAPI
}

func (t TimeStreamWriterMock) WriteRecords(input *timestreamwrite.WriteRecordsInput) (*timestreamwrite.WriteRecordsOutput, error) {
	for _, i := range input.Records {
		if *i.MeasureName == "sample_name_error" {
			return nil, errors.New("error writing to mock timestream database")
		}
	}
	return &timestreamwrite.WriteRecordsOutput{}, nil
}

func Test_writeHandler(t *testing.T) {
	type args struct {
		wr *prompb.WriteRequest
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Write Timestream Request",
			args: args{
				wr: &prompb.WriteRequest{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []*prompb.Label{
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
			want: 200,
		},
		{
			name: "Write Timestream Request With Error",
			args: args{
				wr: &prompb.WriteRequest{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []*prompb.Label{
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
			want: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeRequestMarshaled, err := proto.Marshal(tt.args.wr)
			assert.NoError(t, err)

			writeRequestEncoded := bytes.NewReader(snappy.Encode(nil, writeRequestMarshaled))

			req, err := http.NewRequest("POST", "", writeRequestEncoded)
			assert.NoError(t, err)

			rr := httptest.NewRecorder()
			ad := newTimeStreamAdapter(zap.NewNop().Sugar(), cfg, TimeStreamWriterMock{})
			writeHandler(zap.NewNop().Sugar(), ad).ServeHTTP(rr, req)

			assert.Equal(t, tt.want, rr.Code)
		})
	}
}
