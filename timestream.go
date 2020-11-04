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
	"go.uber.org/zap"
	"math"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/aws/aws-sdk-go/service/timestreamwrite/timestreamwriteiface"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/net/http2"
)

type TimeSteamAdapter struct {
	logger       *zap.SugaredLogger
	tableName    string
	databaseName string
	ttw          timestreamwriteiface.TimestreamWriteAPI
}

func (t TimeSteamAdapter) Write(records []*timestreamwrite.Record) (err error) {
	_, err = t.ttw.WriteRecords(&timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(t.databaseName),
		TableName:    aws.String(t.tableName),
		Records:      records,
	})

	return
}

func (t TimeSteamAdapter) Name() string {
	return "prometheus-timestream-adapter"
}

func newTimeStreamAdapter(logger *zap.SugaredLogger, cfg *config) TimeSteamAdapter {
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

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     aws.String(cfg.awsRegion),
		MaxRetries: aws.Int(10),
		HTTPClient: &http.Client{
			Transport: tr,
		},
	}))

	writeSvc := timestreamwrite.New(sess)

	return TimeSteamAdapter{
		logger:       logger,
		databaseName: cfg.databaseName,
		tableName:    cfg.tableName,
		ttw:          writeSvc,
	}
}

func readLabels(labels []*prompb.Label) (dimensions []*timestreamwrite.Dimension, measureName string) {
	for _, s := range labels {
		if s.Name == "__name__" {
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

func protoToRecords(logger *zap.SugaredLogger, req *prompb.WriteRequest) (records []*timestreamwrite.Record) {
	for _, ts := range req.Timeseries {
		dimensions, measureName := readLabels(ts.Labels)
		for _, s := range ts.Samples {
			switch {
			case math.IsNaN(s.Value) || math.IsInf(s.Value, 0):
				continue
			case len(measureName) >= 62:
				logger.Warnw("Measure name exceeds the maximum supported length", "Measure name", measureName, "Measure name length", len(measureName))
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
