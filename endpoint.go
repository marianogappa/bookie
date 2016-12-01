package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type serverHandler struct {
	db *mariaDB
}

func (h *serverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/fsm"):
		h.fsm(w, r)
	case r.URL.Path == "/latest":
		h.latest(w, r)
	default:
		log.WithFields(log.Fields{"path": r.URL.Path}).Error("unsupported path")
		http.Error(w, "Not found", 404)
	}
}

func (h *serverHandler) fsm(w http.ResponseWriter, r *http.Request) {
	fsmID := r.URL.Query().Get("id")

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

func (h *serverHandler) latest(w http.ResponseWriter, r *http.Request) {
	ns := r.URL.Query().Get("n")

	if ns == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	n, err := strconv.Atoi(ns)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}

	fsms, err := h.db.getLastNFSMs(n)
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
