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
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamquery/timestreamqueryiface"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/aws/aws-sdk-go/service/timestreamwrite/timestreamwriteiface"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type TimeSteamAdapter struct {
	logger       *zap.SugaredLogger
	tableName    string
	databaseName string
	ttw          timestreamwriteiface.TimestreamWriteAPI
	ttq          timestreamqueryiface.TimestreamQueryAPI
}

type queryTask struct {
	query       string
	measureName string
}

func (t *TimeSteamAdapter) Write(records *prompb.WriteRequest) (err error) {
	begin := time.Now()

	wReq := t.protoToRecords(records)
	receivedSamples.Add(float64(len(wReq)))

	_, err = t.ttw.WriteRecords(&timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(t.databaseName),
		TableName:    aws.String(t.tableName),
		Records:      wReq,
	})

	duration := time.Since(begin).Seconds()
	if err != nil {
		t.logger.Warnw("Error sending samples to remote storage", "err", err, "storage", t.Name(), "num_samples", len(wReq))
		failedSamples.WithLabelValues(t.Name()).Add(float64(len(wReq)))
	}
	sentSamples.WithLabelValues(t.Name()).Add(float64(len(wReq)))
	sentBatchDuration.WithLabelValues(t.Name()).Observe(duration)

	return
}

// BuildCommand generates the proper SQL for the query
func (t *TimeSteamAdapter) buildQuery(q *prompb.Query) (task *queryTask, err error) {
	matchers := make([]string, 0, len(q.Matchers))
	task = new(queryTask)
	for _, m := range q.Matchers {
		// Metric Names
		if m.Name == model.MetricNameLabel {
			task.measureName = m.Value
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				matchers = append(matchers, fmt.Sprintf("measure_name = '%s'", m.Value))
			default:
				return nil, errors.Errorf("unsupported match type %v", m.Type)
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
			matchers = append(matchers, fmt.Sprintf("%s LIKE '%s'", m.Name, m.Value))
		case prompb.LabelMatcher_NRE:
			matchers = append(matchers, fmt.Sprintf("%s NOT LIKE '%s'", m.Name, m.Value))
		default:
			return nil, errors.Errorf("unknown match type %v", m.Type)
		}
	}

	matchers = append(matchers, fmt.Sprintf("time BETWEEN from_milliseconds(%d) AND from_milliseconds(%d)", q.StartTimestampMs, q.EndTimestampMs))

	dimensions, err := t.readDimension(task.measureName)

	if err != nil {
		return
	}

	task.query = fmt.Sprintf("SELECT %s, CREATE_TIME_SERIES(time, measure_value::double) AS %s FROM \"%s\".\"%s\" WHERE %s GROUP BY %s",
		strings.Join(dimensions, ", "), task.measureName, cfg.databaseName, cfg.tableName, strings.Join(matchers, " AND "), strings.Join(dimensions, ", "))

	t.logger.Debugw("Timestream read", "query", task.query)

	return
}

func (t TimeSteamAdapter) readDimension(measureName string) (dimensions []string, err error) {
	query := fmt.Sprintf("SHOW MEASURES FROM \"%s\".\"%s\" LIKE '%s'", cfg.databaseName, cfg.tableName, measureName)

	queryOutput, err := t.ttq.Query(&timestreamquery.QueryInput{QueryString: &query})
	if err != nil {
		return
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

func (t *TimeSteamAdapter) handleQueryResult(qo *timestreamquery.QueryOutput, measureName string) (timeSeries []*prompb.TimeSeries, err error) {
	for _, row := range qo.Rows {
		var ts prompb.TimeSeries

		ts.Labels = append(ts.Labels, &prompb.Label{
			Name:  model.MetricNameLabel,
			Value: measureName,
		})

		for i, d := range row.Data {
			if d.ScalarValue != nil {
				ts.Labels = append(ts.Labels, &prompb.Label{
					Name:  *qo.ColumnInfo[i].Name,
					Value: *d.ScalarValue,
				})
			}
			if d.TimeSeriesValue != nil {
				for _, p := range d.TimeSeriesValue {
					value, err := strconv.ParseFloat(*p.Value.ScalarValue, 64)
					if err != nil {
						continue
					}

					s, err := time.Parse("2006-01-02 15:04:05.999999999", *p.Time)

					if err != nil {
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

		timeSeries = append(timeSeries, &ts)
	}
	return
}

func (t TimeSteamAdapter) query(q *prompb.Query) (result prompb.QueryResult, err error) {
	task, err := t.buildQuery(q)

	if err != nil {
		return
	}

	input := &timestreamquery.QueryInput{
		QueryString: &task.query,
	}

	out, err := t.ttq.Query(input)
	if err != nil {
		return
	}

	timeSeries, err := t.handleQueryResult(out, task.measureName)

	if err != nil {
		return
	}

	result = prompb.QueryResult{
		Timeseries: timeSeries,
	}

	return
}

func (t *TimeSteamAdapter) Read(request *prompb.ReadRequest) (response *prompb.ReadResponse, err error) {
	var queryResult prompb.QueryResult
	var queryResults []*prompb.QueryResult

	for _, q := range request.Queries {
		queryResult, err = t.query(q)

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

func (t TimeSteamAdapter) Name() string {
	return "prometheus-timestream-adapter"
}

func newTimeStreamAdapter(logger *zap.SugaredLogger, cfg *config, writeSvc timestreamwriteiface.TimestreamWriteAPI, readSvc timestreamqueryiface.TimestreamQueryAPI) TimeSteamAdapter {
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		// Using DefaultTransport values for other parameters: https://golang.org/pkg/net/http/#RoundTripper
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// So client makes HTTP/2 requests
	http2.ConfigureTransport(tr)

	if writeSvc == nil || readSvc == nil {
		sess := session.Must(session.NewSession(
			&aws.Config{
				Region:     aws.String(cfg.awsRegion),
				MaxRetries: aws.Int(10),
				HTTPClient: &http.Client{
					Transport: tr,
				},
			},
		))

		if writeSvc == nil {
			writeSvc = timestreamwrite.New(sess)
		}

		if readSvc == nil {
			readSvc = timestreamquery.New(sess)
		}
	}

	return TimeSteamAdapter{
		logger:       logger,
		databaseName: cfg.databaseName,
		tableName:    cfg.tableName,
		ttw:          writeSvc,
		ttq:          readSvc,
	}
}

func (t TimeSteamAdapter) readLabels(labels []*prompb.Label) (dimensions []*timestreamwrite.Dimension, measureName string) {
	for _, s := range labels {
		if s.Name == model.MetricNameLabel {
			measureName = s.Value
			continue
		}
		dimensions = append(dimensions, &timestreamwrite.Dimension{
			Name:  aws.String(s.Name),
			Value: aws.String(s.Value),
		})
	}

	return
}

func (t TimeSteamAdapter) protoToRecords(req *prompb.WriteRequest) (records []*timestreamwrite.Record) {
	for _, ts := range req.Timeseries {
		dimensions, measureName := t.readLabels(ts.Labels)
		for _, s := range ts.Samples {
			switch {
			case math.IsNaN(s.Value) || math.IsInf(s.Value, 0):
				continue
			case len(measureName) >= 62:
				t.logger.Warnw("Measure name exceeds the maximum supported length", "Measure name", measureName, "Length", len(measureName))
				continue
			}

			records = append(records, &timestreamwrite.Record{
				Dimensions:       dimensions,
				MeasureName:      aws.String(measureName),
				MeasureValueType: aws.String("DOUBLE"),
				MeasureValue:     aws.String(fmt.Sprint(s.Value)),
				Time:             aws.String(fmt.Sprint(s.Timestamp)),
				TimeUnit:         aws.String("MILLISECONDS"),
			})
		}
	}

	return
}
