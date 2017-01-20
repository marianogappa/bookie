package main

import "github.com/prometheus/client_golang/prometheus"

var (
	promBookieRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "bookie_requests",
			Help: "Total number of API requests",
		},
	)

	promLastScrapedOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bookie_last_scraped_offset",
			Help: "Last scaped offset for topic x partition",
		},
		[]string{"topic", "partition"},
	)
)

func init() {
	prometheus.MustRegister(promBookieRequests)
	prometheus.MustRegister(promLastScrapedOffset)
}
