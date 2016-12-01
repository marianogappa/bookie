package main

import (
	"encoding/json"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

type serverHandler struct {
	db *mariaDB
}

func (h *serverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fsmID := r.URL.Query().Get("fsmID")

	if fsmID == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	fsms, err := h.db.findFSM(fsmID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	byts, err := json.Marshal(fsms)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	_, err = w.Write(byts)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Error("Couldn't reply to fsmID request")
	}
}

func mustServeBookie(addr string, db *mariaDB) {
	mux := http.NewServeMux()
	sh := &serverHandler{db: db}

	mux.Handle("/", sh)
	log.WithFields(log.Fields{"address": addr}).Info("Serving bookie.")
	log.Fatal(http.ListenAndServe(addr, mux))
}
