package main

import (
	"fmt"
	"net/http"

	_ "net/http/pprof"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
)

type healthCheckHandler struct{}

func (_ *healthCheckHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
}

func mustServeSnitch(addr string) {
	mux := http.NewServeMux()
	hc := &healthCheckHandler{}

	go func() {
		log.Info("Serving pprof on 0.0.0.0:6060")
		log.Fatal(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	mux.Handle("/metrics", prometheus.Handler())
	mux.Handle("/health_check", hc)
	log.WithFields(log.Fields{"address": addr}).Info("Serving snitch.")
	log.Fatal(http.ListenAndServe(addr, mux))
}
