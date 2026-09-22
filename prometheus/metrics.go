package prometheus

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsFactory struct {
	ServiceName          string
	ProcessedEventsTotal *prometheus.CounterVec
	E2ELatencyHistogram  prometheus.Histogram
	ProcessingLagGauge   *prometheus.GaugeVec
}

func NewMetricsFactory(serviceName string) *MetricsFactory {
	metrics := &MetricsFactory{
		ServiceName: serviceName,
		ProcessedEventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: serviceName,
				Name:      "processed_events_total",
				Help:      "Totale eventi elaborati",
			},
			[]string{"type", "status"}),
		E2ELatencyHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: serviceName,
				Name:      "e2e_latency_seconds",
				Help:      "Latenza End-to-End in secondi.",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
			},
		),
		ProcessingLagGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: serviceName,
				Name:      "processing_lag_seconds",
				Help:      "Ritardo di elaborazione (lag) per sensore.",
			},
			[]string{"sensor_id"},
		),
	}

	prometheus.MustRegister(metrics.ProcessedEventsTotal)
	prometheus.MustRegister(metrics.E2ELatencyHistogram)
	prometheus.MustRegister(metrics.ProcessingLagGauge)

	// Inizializza subito i contatori principali a 0 per farli apparire su Prometheus
	metrics.ProcessedEventsTotal.WithLabelValues("LATENCY_MARKER", "success").Add(0)
	metrics.ProcessedEventsTotal.WithLabelValues("DATA", "success").Add(0)
	metrics.ProcessedEventsTotal.WithLabelValues("DATA", "unmarshal_payload_error").Add(0)
	metrics.ProcessedEventsTotal.WithLabelValues("DATA", "redis_config_missing").Add(0)

	return metrics
}

// StartMetricsServer avvia lo endpoint /metrics su porta dedicata
func StartMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Printf("[METRICS] Servizio metriche in ascolto su %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("[METRICS] Errore server metriche: %v", err)
		}
	}()
}
