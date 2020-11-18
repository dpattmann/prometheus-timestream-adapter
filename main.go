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
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

type config struct {
	awsRegion     string
	databaseName  string
	listenAddr    string
	tableName     string
	telemetryPath string
}

var (
	cfg = new(config)

	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	sentSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
		[]string{"remote"},
	)
	failedSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
		[]string{"remote"},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote"},
	)
)

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)

	flag.StringVar(&cfg.awsRegion, "awsRegion", "eu-central-1", "")
	flag.StringVar(&cfg.databaseName, "databaseName", "prometheus-database", "")
	flag.StringVar(&cfg.listenAddr, "listenAddr", ":9201", "")
	flag.StringVar(&cfg.tableName, "tableName", "prometheus-table", "")
	flag.StringVar(&cfg.telemetryPath, "telemetryPath", "/metric", "")

	flag.Parse()
}

func main() {
	zapConfig := zap.NewProductionConfig()
	zapConfig.DisableStacktrace = true
	sugarLogger, err := zapConfig.Build()

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	defer sugarLogger.Sync() // flushes buffer, if any
	sugar := sugarLogger.Sugar()

	timeStreamAdapter := newTimeStreamAdapter(sugar, cfg, nil)
	if err := serve(sugar, cfg.listenAddr, timeStreamAdapter); err != nil {
		sugar.Errorw("Failed to listen", "addr", cfg.listenAddr, "err", err)
		os.Exit(1)
	}
}

type adapter interface {
	Write(records []*timestreamwrite.Record) error
	Name() string
}

func serve(logger *zap.SugaredLogger, addr string, ad adapter) error {
	http.Handle(cfg.telemetryPath, promhttp.Handler())
	http.Handle("/write", writeHandler(logger, ad))

	return http.ListenAndServe(addr, nil)
}

func sendRecords(logger *zap.SugaredLogger, ad adapter, records []*timestreamwrite.Record) (err error) {
	begin := time.Now()
	err = ad.Write(records)
	duration := time.Since(begin).Seconds()
	if err != nil {
		logger.Warnw("Error sending samples to remote storage", "err", err, "storage", ad.Name(), "num_samples", len(records))
		failedSamples.WithLabelValues(ad.Name()).Add(float64(len(records)))
	}
	sentSamples.WithLabelValues(ad.Name()).Add(float64(len(records)))
	sentBatchDuration.WithLabelValues(ad.Name()).Observe(duration)

	return
}
