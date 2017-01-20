package main

import (
	"fmt"
	"net/http"

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

	mux.Handle("/metrics", prometheus.Handler())
	mux.Handle("/health_check", hc)
	log.WithFields(log.Fields{"address": addr}).Info("Serving snitch.")
	log.Fatal(http.ListenAndServe(addr, mux))
}
