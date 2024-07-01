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
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

func writeHandler(ctx context.Context, logger *zap.SugaredLogger, ad PrometheusRemoteStorageAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorw("Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			logger.Errorw("Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			logger.Errorw("Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = ad.Write(ctx, &req)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error writing to AWS Timestream"))
		}
	}
}

func readHandler(ctx context.Context, logger *zap.SugaredLogger, ad PrometheusRemoteStorageAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorw("Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			logger.Errorw("Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			logger.Errorw("Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = ad.Read(ctx, &req)
		if err != nil {
			logger.Errorw("Error executing query", "query", req, "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			logger.Errorw("Error writing response", "err", err)
		}
	}
}

func healthHandler(logger *zap.SugaredLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Encoding", "UTF-8")

		if _, err := w.Write([]byte("Ok")); err != nil {
			logger.Errorw("Error writing response", "err", err)
		}
	}
}
