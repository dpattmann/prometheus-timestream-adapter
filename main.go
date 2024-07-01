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
	"log"
	"net/http"
	"os"

	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	flag "github.com/spf13/pflag"
)

type adapterCfg struct {
	help          bool
	awsRegion     string
	databaseName  string
	listenAddr    string
	logLevel      string
	tableName     string
	telemetryPath string
	tls           bool
	tlsCert       string
	tlsKey        string
}

var (
	cfg = new(adapterCfg)

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
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(sentSamples)

	flag.BoolVar(&cfg.help, "help", false, "")
	flag.BoolVar(&cfg.tls, "tls", false, "")
	flag.StringVar(&cfg.awsRegion, "awsRegion", "eu-central-1", "")
	flag.StringVar(&cfg.databaseName, "databaseName", "prometheus-database", "")
	flag.StringVar(&cfg.listenAddr, "listenAddr", ":9201", "")
	flag.StringVar(&cfg.logLevel, "logLevel", "error", "")
	flag.StringVar(&cfg.tableName, "tableName", "prometheus-table", "")
	flag.StringVar(&cfg.telemetryPath, "telemetryPath", "/metric", "")
	flag.StringVar(&cfg.tlsCert, "tlsCert", "tls.cert", "")
	flag.StringVar(&cfg.tlsKey, "tlsKey", "tls.key", "")

	flag.Parse()
}

func main() {
	if cfg.help {
		flag.PrintDefaults()
		return
	}

	ctx := context.Background()

	zapConfig := zap.NewProductionConfig()
	zapConfig.DisableStacktrace = true

	switch cfg.logLevel {
	case "warning":
		zapConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		zapConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "info":
		zapConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "debug":
		zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	default:
		os.Exit(1)
	}

	sugarLogger, err := zapConfig.Build()

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	defer sugarLogger.Sync() // flushes buffer, if any
	sugar := sugarLogger.Sugar()

	sugar.Debugf("Sending logs to timeseries database %s in AWS Region %s", cfg.databaseName, cfg.awsRegion)

	timeStreamAdapter := newTimeStreamAdapter(ctx, sugar, cfg, newTimestreamQueryPaginator, nil, nil)
	if err := serve(ctx, sugar, cfg.listenAddr, timeStreamAdapter); err != nil {
		sugar.Errorw("Failed to listen", "addr", cfg.listenAddr, "err", err)
		os.Exit(1)
	}
}

type PrometheusRemoteStorageAdapter interface {
	Write(ctx context.Context, records *prompb.WriteRequest) error
	Read(ctx context.Context, request *prompb.ReadRequest) (*prompb.ReadResponse, error)
	Name() string
}

func serve(ctx context.Context, logger *zap.SugaredLogger, addr string, storageAdapter PrometheusRemoteStorageAdapter) error {
	http.Handle(cfg.telemetryPath, promhttp.Handler())
	http.Handle("/write", writeHandler(ctx, logger, storageAdapter))
	http.Handle("/read", readHandler(ctx, logger, storageAdapter))
	http.Handle("/health", healthHandler(logger))

	if cfg.tls {
		logger.Debugf("Listening on port %s with cert %s and key %s", addr, cfg.tlsCert, cfg.tlsKey)
		return http.ListenAndServeTLS(addr, cfg.tlsCert, cfg.tlsKey, nil)
	}
	logger.Debugf("Listening on port %s", addr)
	return http.ListenAndServe(addr, nil)
}
