package main

import (
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

func writeHandler(logger *zap.SugaredLogger, ad adapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
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

		records := protoToRecords(logger, &req)
		receivedSamples.Add(float64(len(records)))

		err = sendRecords(logger, ad, records)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error writing to AWS Timestream"))
		}
	}
}
