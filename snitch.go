package main

import (
	"fmt"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

type healthCheckHandler struct{}

func (_ *healthCheckHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
}

func mustServeSnitch(addr string) {
	mux := http.NewServeMux()
	hc := &healthCheckHandler{}

	mux.Handle("/health_check", hc)
	log.WithFields(log.Fields{"address": addr}).Info("Serving snitch.")
	log.Fatal(http.ListenAndServe(addr, mux))
}
