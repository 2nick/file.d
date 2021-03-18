package pipeline

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metricsHolder struct {
	pipelineName       string
	metricsGen         int // generation is used to drop unused metrics from counters
	metricsGenTime     time.Time
	metricsGenInterval time.Duration
	metrics            []*metrics
	registry           *prometheus.Registry
}

type metrics struct {
	name   string
	labels []string

	root *mNode

	currentEventsTotal  *prometheus.CounterVec
	previousEventsTotal *prometheus.CounterVec

	currentEventSizeSummary  *prometheus.SummaryVec
	previousEventSizeSummary *prometheus.SummaryVec
}

type mNode struct {
	childs map[string]*mNode
	mu     *sync.RWMutex
	self   string
}

func newMetricsHolder(pipelineName string, registry *prometheus.Registry, metricsGenInterval time.Duration) *metricsHolder {
	return &metricsHolder{
		pipelineName: pipelineName,
		registry:     registry,

		metrics:            make([]*metrics, 0, 0),
		metricsGenInterval: metricsGenInterval,
	}

}

func (m *metricsHolder) AddAction(metricName string, metricLabels []string) {
	m.metrics = append(m.metrics, &metrics{
		name:   metricName,
		labels: metricLabels,
		root: &mNode{
			childs: make(map[string]*mNode),
			mu:     &sync.RWMutex{},
		},
		currentEventsTotal:       nil,
		previousEventsTotal:      nil,
		currentEventSizeSummary:  nil,
		previousEventSizeSummary: nil,
	})
}

func (m *metricsHolder) start() {
	m.nextMetricsGen()
}

func (m *metricsHolder) nextMetricsGen() {
	metricsGen := strconv.Itoa(m.metricsGen)

	for index, metrics := range m.metrics {
		if metrics.name == "" {
			continue
		}

		etOpts := prometheus.CounterOpts{
			Namespace:   "file_d",
			Subsystem:   "pipeline_" + m.pipelineName,
			Name:        metrics.name + "_events_total",
			Help:        fmt.Sprintf("how many events processed by pipeline %q and #%d action", m.pipelineName, index),
			ConstLabels: map[string]string{"gen": metricsGen},
		}
		counterEventsTotal := prometheus.NewCounterVec(etOpts, append([]string{"status"}, metrics.labels...))
		obsoleteEventsTotal := metrics.previousEventsTotal

		metrics.previousEventsTotal = metrics.currentEventsTotal
		metrics.currentEventsTotal = counterEventsTotal

		m.registry.MustRegister(counterEventsTotal)
		if obsoleteEventsTotal != nil {
			m.registry.Unregister(obsoleteEventsTotal)
		}

		esOpts := prometheus.SummaryOpts{
			Namespace:   "file_d",
			Subsystem:   "pipeline_" + m.pipelineName,
			Name:        metrics.name + "_event_size_bytes",
			Help:        fmt.Sprintf("sizes of events processed by pipeline %q and #%d action", m.pipelineName, index),
			ConstLabels: map[string]string{"gen": metricsGen},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}
		currentEventSizeSummary := prometheus.NewSummaryVec(esOpts, append([]string{"status"}, metrics.labels...))
		obsoleteEventSizeSummary := metrics.previousEventSizeSummary

		metrics.previousEventSizeSummary = metrics.currentEventSizeSummary
		metrics.currentEventSizeSummary = currentEventSizeSummary

		m.registry.MustRegister(currentEventSizeSummary)
		if obsoleteEventSizeSummary != nil {
			m.registry.Unregister(obsoleteEventSizeSummary)
		}
	}

	m.metricsGen++
	m.metricsGenTime = time.Now()
}

func (m *metricsHolder) count(event *Event, actionIndex int, eventStatus eventStatus, valuesBuf []string) []string {
	if len(m.metrics) == 0 {
		return valuesBuf
	}

	metrics := m.metrics[actionIndex]
	if metrics.name == "" {
		return valuesBuf
	}

	valuesBuf = valuesBuf[:0]
	valuesBuf = append(valuesBuf, string(eventStatus))

	mn := metrics.root
	for _, field := range metrics.labels {
		val := DefaultFieldValue

		node := event.Root.Dig(field)
		if node != nil {
			val = node.AsString()
		}

		mn.mu.RLock()
		nextMN, has := mn.childs[val]
		mn.mu.RUnlock()

		if !has {
			mn.mu.Lock()
			nextMN, has = mn.childs[val]
			if !has {
				key := DefaultFieldValue
				if node != nil {
					key = string(node.AsBytes()) // make string from []byte to make map string keys works good
				}

				nextMN = &mNode{
					childs: make(map[string]*mNode),
					self:   key,
					mu:     &sync.RWMutex{},
				}
				mn.childs[key] = nextMN
			}
			mn.mu.Unlock()
		}

		valuesBuf = append(valuesBuf, nextMN.self)
		mn = nextMN
	}

	metrics.currentEventsTotal.WithLabelValues(valuesBuf...).Inc()
	metrics.currentEventSizeSummary.WithLabelValues(valuesBuf...).Observe(float64(event.Size))

	return valuesBuf
}

func (m *metricsHolder) maintenance() {
	if time.Now().Sub(m.metricsGenTime) < metricsGenInterval {
		return
	}

	m.nextMetricsGen()
}
