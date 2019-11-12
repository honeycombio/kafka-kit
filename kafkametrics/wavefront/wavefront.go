// Package wavefront implements
// a kafkametrics Handler.
package wavefront

import (
	"fmt"
	"regexp"
	"time"

	"github.com/honeycombio/kafka-kit/kafkametrics"

	wf "github.com/spaceapegames/go-wavefront"
)

// Config holds Handler
// configuration parameters.
type Config struct {
	// Wavefront API key
	APIKey string
	// NetworkTXQuery is a query string that
	// should return the outbound network metrics
	// by host for the reference Kafka brokers.
	// For example: `rate(ts(kafka.server.brokertopics.bytesoutpersec.Count, env=dogfood))`
	NetworkTXQuery string
	// BrokerIDTag is the host tag name
	// for Kafka broker IDs. (e.g. `kafka`)
	BrokerIDTag string
	// BrokerIDTag is the host tag name
	// for the current environment. (e.g. `dogfood`)
	Environment string
	// MetricsWindow specifies the window size of
	// timeseries data to evaluate in seconds.
	// All values for the window are averaged.
	MetricsWindow int
}

type wfHandler struct {
	c             *wf.Client
	netTXQuery    string
	brokerIDTag   string
	environment   string
	metricsWindow int
	tagCache      map[string][]string
	keysRegex     *regexp.Regexp
	redactionSub  []byte
}

// NewHandler takes a *Config and
// returns a Handler, along with
// any credential validation errors.
// Further backends can be supported with
// a type switch and some other changes.
func NewHandler(c *Config) (kafkametrics.Handler, error) {
	h := &wfHandler{
		netTXQuery:    createNetTXQuery(c),
		metricsWindow: c.MetricsWindow,
		brokerIDTag:   c.BrokerIDTag,
		tagCache:      make(map[string][]string),
		keysRegex:     keysRegex,
	}

	client := wf.NewClient(&f.Config{
		Address: fmt.Sprintf("wfproxy.int.%s.honeycomb.io", c.Environment),
		Token:   c.APIKey,
	})

	h.c = client

	return h, nil
}

// PostEvent is a no-op as Wavefront doesn't support Markers.
// TODO(lizf): make this do Honeycomb markers instead.
func (h *wfHandler) PostEvent(e *kafkametrics.Event) error {
	return nil
}

// GetMetrics requests broker metrics and metadata
// from the Datadog API and returns a BrokerMetrics.
// If any errors are encountered (i.e. complete metadata
// for a given broker cann't be retrieved), the broker
// will not be included in the BrokerMetrics.
func (h *wfHandler) GetMetrics() (kafkametrics.BrokerMetrics, []error) {
	var errors []error

	// Get series.
	start := time.Now().Add(-time.Duration(h.metricsWindow) * time.Second).Unix()
	o, err := h.c.QueryMetrics(start, time.Now().Unix(), h.netTXQuery)
	if err != nil {
		return nil, []error{&kafkametrics.APIError{
			Request: "metrics query",
			Message: h.scrubbedErrorText(err),
		}}
	}

	if len(o) == 0 {
		return nil, []error{&kafkametrics.NoResults{
			Message: fmt.Sprintf("No data returned with query %s", h.netTXQuery),
		}}
	}

	// Get a []*kafkametrics.Broker from the series.
	// Brokers with missing points are excluded
	// from blist.
	blist, errs := brokersFromSeries(o)
	if errs != nil {
		errors = append(errors, errs...)
	}

	// The []*kafkametrics.Broker only contains hostnames
	// and the network tx metric. Fetch the rest
	// of the required metadata and construct
	// a kafkametrics.BrokerMetrics.
	bm, errs := h.brokerMetricsFromList(blist)
	if errs != nil {
		errors = append(errors, errs...)
	}

	return bm, errors
}
