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
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	writetypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

const TimestreamMaxRecordsPerRequest = 100

type TimestreamQueryApi interface {
	Query(ctx context.Context, params *timestreamquery.QueryInput, optFns ...func(*timestreamquery.Options)) (*timestreamquery.QueryOutput, error)
}

type TimestreamWriteApi interface {
	WriteRecords(ctx context.Context, params *timestreamwrite.WriteRecordsInput, optFns ...func(*timestreamwrite.Options)) (*timestreamwrite.WriteRecordsOutput, error)
}

type NewQueryPaginator func(client TimestreamQueryApi, params *timestreamquery.QueryInput, optFns ...func(*timestreamquery.QueryPaginatorOptions)) PaginatorApi

type PaginatorApi interface {
	HasMorePages() bool
	NextPage(ctx context.Context, optFns ...func(*timestreamquery.Options)) (*timestreamquery.QueryOutput, error)
}

type TimeStreamAdapter struct {
	databaseName string
	logger       *zap.SugaredLogger
	tableName    string
	TimestreamQueryApi
	TimestreamWriteApi
	NewQueryPaginator
}

type queryTask struct {
	query       string
	measureName string
}

type writeTask struct {
	measureName string
	dimensions  []writetypes.Dimension
}

func newTimestreamQueryPaginator(client TimestreamQueryApi, params *timestreamquery.QueryInput, optFns ...func(*timestreamquery.QueryPaginatorOptions)) PaginatorApi {
	return timestreamquery.NewQueryPaginator(client, params, optFns...)
}

func newTimeStreamAdapter(ctx context.Context, logger *zap.SugaredLogger, cfg *adapterCfg, newQueryPaginator NewQueryPaginator, writeSvc *timestreamwrite.Client, querySvc *timestreamquery.Client) TimeStreamAdapter {
	if writeSvc == nil || querySvc == nil {
		client := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			tr.ResponseHeaderTimeout = 20 * time.Second

			// Enable HTTP/2
			err := http2.ConfigureTransport(tr)
			if err != nil {
				logger.Fatalw("Unable to configure HTTP/2 transport", "error", err)
			}
		})

		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.awsRegion), config.WithHTTPClient(client), config.WithRetryMaxAttempts(10))
		if err != nil {
			logger.Fatalw("Unable to load AWS SDK config", "error", err)
		}

		if writeSvc == nil {
			writeSvc = timestreamwrite.NewFromConfig(awsCfg)
		}

		if querySvc == nil {
			querySvc = timestreamquery.NewFromConfig(awsCfg)
		}
	}

	return TimeStreamAdapter{
		TimestreamQueryApi: querySvc,
		TimestreamWriteApi: writeSvc,
		NewQueryPaginator:  newQueryPaginator,
		databaseName:       cfg.databaseName,
		logger:             logger,
		tableName:          cfg.tableName,
	}
}

// Write implementation

func (t TimeStreamAdapter) Name() string {
	return "prometheus-timestream-adapter"
}

func (t TimeStreamAdapter) Write(ctx context.Context, req *prompb.WriteRequest) (err error) {
	timer := prometheus.NewTimer(sentBatchDuration.WithLabelValues(t.Name()))
	defer timer.ObserveDuration()

	records := t.toRecords(req)
	receivedSamples.Add(float64(len(records)))

	for _, chunk := range t.splitRecords(records) {
		_, err = t.WriteRecords(ctx, &timestreamwrite.WriteRecordsInput{
			DatabaseName: aws.String(t.databaseName),
			TableName:    aws.String(t.tableName),
			Records:      chunk,
		})

		if err != nil {
			t.logger.Warnw("Error sending samples to remote storage", "err", err, "storage", t.Name(), "num_samples", len(chunk))
			failedSamples.WithLabelValues(t.Name()).Add(float64(len(chunk)))
		}
	}
	sentSamples.WithLabelValues(t.Name()).Add(float64(len(records)))

	return
}

func (t TimeStreamAdapter) allCharactersValid(str string) bool {
	isValid := func(char rune) bool {
		return (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') ||
			char == '_' || char == '-' || char == '.'
	}
	for _, char := range str {
		if !isValid(char) {
			return false
		}
	}
	return true
}

func (t TimeStreamAdapter) toRecords(writeRequest *prompb.WriteRequest) (records []writetypes.Record) {
	for _, ts := range writeRequest.Timeseries {
		task := t.readLabels(ts.Labels)
		for _, s := range ts.Samples {
			switch {
			case math.IsNaN(s.Value) || math.IsInf(s.Value, 0):
				continue
			case len(task.measureName) >= 62:
				t.logger.Warnw("Measure name exceeds the maximum supported length", "Measure name", task.measureName, "Length", len(task.measureName))
				continue
			case !t.allCharactersValid(task.measureName):
				t.logger.Warnw("Measure name contains illegal characters", "Measure name", task.measureName)
				continue
			}

			records = append(records, writetypes.Record{
				Dimensions:       task.dimensions,
				MeasureName:      aws.String(task.measureName),
				MeasureValueType: writetypes.MeasureValueTypeDouble,
				MeasureValue:     aws.String(fmt.Sprint(s.Value)),
				Time:             aws.String(fmt.Sprint(s.Timestamp)),
				TimeUnit:         writetypes.TimeUnitMilliseconds,
			})
		}
	}

	return
}

func (t TimeStreamAdapter) splitRecords(records []writetypes.Record) [][]writetypes.Record {
	var chunked [][]writetypes.Record

	for i := 0; i < len(records); i += TimestreamMaxRecordsPerRequest {
		end := i + TimestreamMaxRecordsPerRequest
		if end > len(records) {
			end = len(records)
		}

		chunked = append(chunked, records[i:end])
	}
	t.logger.Debugf("Successfully split %d records into %d chunks", len(records), len(chunked))
	return chunked
}

// Read implementation

func (t TimeStreamAdapter) Read(ctx context.Context, request *prompb.ReadRequest) (response *prompb.ReadResponse, err error) {
	var queryResult prompb.QueryResult
	var queryResults []*prompb.QueryResult

	for _, q := range request.Queries {
		queryResult, err = t.runReadRequestQuery(ctx, q)

		if err != nil {
			return
		}

		queryResults = append(queryResults, &queryResult)
	}

	response = &prompb.ReadResponse{
		Results: queryResults,
	}

	return
}

func (t TimeStreamAdapter) runReadRequestQuery(ctx context.Context, q *prompb.Query) (result prompb.QueryResult, err error) {
	task, err := t.buildTimeStreamQueryString(ctx, q)

	if err != nil {
		return
	}

	var timeSeries []*prompb.TimeSeries
	paginator := t.NewQueryPaginator(t, &timestreamquery.QueryInput{
		QueryString: aws.String(task.query),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return result, err
		}

		timeSeries = t.handleQueryResult(page, timeSeries, task.measureName)
	}

	result = prompb.QueryResult{
		Timeseries: timeSeries,
	}

	return
}

// BuildCommand generates the proper SQL for the runReadRequestQuery
func (t TimeStreamAdapter) buildTimeStreamQueryString(ctx context.Context, q *prompb.Query) (task queryTask, err error) {
	matchers := make([]string, 0, len(q.Matchers))
	for _, m := range q.Matchers {
		// Metric Names
		if m.Name == model.MetricNameLabel {
			task.measureName = m.Value
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				matchers = append(matchers, fmt.Sprintf("measure_name = '%s'", m.Value))
			default:
				err = errors.Errorf("unsupported match type %v", m.Type)
				return
			}
			continue
		}

		// Labels
		switch m.Type {
		case prompb.LabelMatcher_EQ:
			matchers = append(matchers, fmt.Sprintf("%s = '%s'", m.Name, m.Value))
		case prompb.LabelMatcher_NEQ:
			matchers = append(matchers, fmt.Sprintf("%s != '%s'", m.Name, m.Value))
		case prompb.LabelMatcher_RE:
			matchers = append(matchers, fmt.Sprintf("regexp_like(%s, '%s')", m.Name, m.Value))
		case prompb.LabelMatcher_NRE:
			matchers = append(matchers, fmt.Sprintf("NOT regexp_like(%s, '%s')", m.Name, m.Value))
		default:
			err = errors.Errorf("unknown match type %v", m.Type)
			return
		}
	}

	matchers = append(matchers, fmt.Sprintf("time BETWEEN from_milliseconds(%d) AND from_milliseconds(%d)",
		q.StartTimestampMs, q.EndTimestampMs))

	dimensions, err := t.readDimension(ctx, task.measureName)

	if err != nil {
		return
	}

	task.query = fmt.Sprintf("SELECT %s, CREATE_TIME_SERIES(time, measure_value::double) AS %s FROM \"%s\".\"%s\" WHERE %s GROUP BY %s",
		strings.Join(dimensions, ", "), task.measureName, cfg.databaseName, cfg.tableName, strings.Join(matchers, " AND "), strings.Join(dimensions, ", "))

	t.logger.Debugw("Timestream read", "runReadRequestQuery", task.query)

	return
}

func (t TimeStreamAdapter) readDimension(ctx context.Context, measureName string) (dimensions []string, err error) {
	query := fmt.Sprintf("SHOW MEASURES FROM \"%s\".\"%s\" LIKE '%s'", cfg.databaseName, cfg.tableName, measureName)

	queryOutput, err := t.Query(ctx, &timestreamquery.QueryInput{QueryString: &query})
	if err != nil {
		return
	}

	if len(queryOutput.Rows) == 0 {
		return nil, errors.New("No measure found")
	}

	for i, q := range queryOutput.ColumnInfo {
		if *q.Name == "dimensions" {
			for _, rv := range queryOutput.Rows[0].Data[i].ArrayValue {
				for _, d := range rv.RowValue.Data {
					if *d.ScalarValue != "varchar" {
						dimensions = append(dimensions, *d.ScalarValue)
					}
				}
			}
		}
	}

	return
}

func (t TimeStreamAdapter) readLabels(labels []prompb.Label) (task writeTask) {
	for _, s := range labels {
		if s.Name == model.MetricNameLabel {
			task.measureName = s.Value
			continue
		}
		task.dimensions = append(task.dimensions, writetypes.Dimension{
			Name:  aws.String(s.Name),
			Value: aws.String(s.Value),
		})
	}

	return
}

func (t TimeStreamAdapter) handleQueryResult(qo *timestreamquery.QueryOutput, timeSeries []*prompb.TimeSeries, measureName string) []*prompb.TimeSeries {
	for _, row := range qo.Rows {
		var ts prompb.TimeSeries

		ts.Labels = append(ts.Labels, prompb.Label{
			Name:  model.MetricNameLabel,
			Value: measureName,
		})

		for i, d := range row.Data {
			if d.ScalarValue != nil {
				ts.Labels = append(ts.Labels, prompb.Label{
					Name:  *qo.ColumnInfo[i].Name,
					Value: *d.ScalarValue,
				})
			}
			if d.TimeSeriesValue != nil {
				for _, p := range d.TimeSeriesValue {
					value, err := strconv.ParseFloat(*p.Value.ScalarValue, 64)
					if err != nil {
						t.logger.Warnw("Can't convert scalar value")
						continue
					}

					s, err := time.Parse("2006-01-02 15:04:05.999999999", *p.Time)
					if err != nil {
						t.logger.Warnw("Can't parse time")
						continue
					}

					t := s.UnixNano() / int64(time.Millisecond)
					ts.Samples = append(ts.Samples, prompb.Sample{
						Value:     value,
						Timestamp: t,
					})
				}
			}
		}
		receivedSamples.Add(float64(len(ts.Samples)))
		timeSeries = append(timeSeries, &ts)
	}

	return timeSeries
}
