package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/honeycombio/kafka-kit/kafkazk"
	"github.com/jamiealquiza/envy"
	wf "github.com/spaceapegames/go-wavefront"
)

// Config holds
// config parameters.
type Config struct {
	Client      *wf.Client
	APIKey      string
	PartnQuery  string
	BrokerQuery string
	BrokerIDTag string
	Span        int
	ZKAddr      string
	ZKPrefix    string
	Verbose     bool
	DryRun      bool
	Compression bool
}

var config = &Config{} // :(

func init() {
	flag.StringVar(&config.APIKey, "api-key", "", "Wavefront API key")
	bq := flag.String("broker-storage-query", "ts(disk.free, env=production and aws_role='kafka' and path='/var/lib/kafka')", "Wavefront metric query to get broker storage free.")
	pq := flag.String("partition-size-query", "max(ts(kafka.server.brokertopics.size.Value, env=production), topic, partition)", "Wavefront query to get partition size by topic, partition")
	flag.IntVar(&config.Span, "span", 3600, "Query range in seconds (now - span)")
	flag.StringVar(&config.ZKAddr, "zk-addr", "zk1:2181,zk2:2181,zk3:2181", "ZooKeeper connect string")
	flag.StringVar(&config.ZKPrefix, "zk-prefix", "topicmappr", "ZooKeeper namespace prefix")
	flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Dry run mode (don't reach Zookeeper)")
	flag.BoolVar(&config.Compression, "compression", true, "Whether to compress metrics data written to ZooKeeper")

	envy.Parse("METRICSFETCHER")
	flag.Parse()

	// Complete query string.
	config.BrokerQuery = client.NewQuery(wavefront.NewQueryParams(fmt.Sprintf(*bq, config.BrokerIDTag))
	config.PartnQuery.SetStartTime(int64(config.Span * time.Second))
	config.PartnQuery = client.NewQuery(wavefront.NewQueryParams(*pq))
	config.PartnQuery.SetStartTime(int64(config.Span * time.Second)
}

func main() {
	// Init, validate wf client.
	config.Client = wf.NewClient(&wf.Config{
    Address: fmt.Sprintf("wfproxy.int.%s.honeycomb.io", c.Environment),
    Token:   config.APIKey,
  }
	ok, err := config.Client.Validate()
	exitOnErr(err)

	if !ok {
		exitOnErr(errors.New("Invalid API or app key"))
	}

	// Init ZK client.
	var zk kafkazk.Handler
	if !config.DryRun {
		zk, err = kafkazk.NewHandler(&kafkazk.Config{
			Connect: config.ZKAddr,
		})
		exitOnErr(err)
	}

	// Ensure znodes exist.
	paths := zkPaths(config.ZKPrefix)
	if !config.DryRun {
		err = createZNodesIfNotExist(zk, paths)
		exitOnErr(err)
	}

	// Fetch metrics data.
	fmt.Printf("Submitting %s\n", config.PartnQuery)
	pm, err := partitionMetrics(config)
	exitOnErr(err)
	fmt.Println("success")

	partnData, err := json.Marshal(pm)
	exitOnErr(err)

	fmt.Printf("Submitting %s\n", config.BrokerQuery)
	bm, err := brokerMetrics(config)
	exitOnErr(err)
	fmt.Println("success")

	brokerData, err := json.Marshal(bm)
	exitOnErr(err)

	// Trunc the paths slice if
	// there's a prefix.
	if len(paths) == 3 {
		paths = paths[1:]
	}

	if config.Verbose {
		fmt.Printf("Broker data (will store at %s, query %s):\n%s\n"+
			"Partition data (will store at %s, query %s):\n%s\n",
			paths[1], config.BrokerQuery, brokerData,
			paths[0], config.PartnQuery, partnData)
	}

	if config.DryRun {
		return
	}

	// Write to ZK.
	for i, data := range [][]byte{partnData, brokerData} {
		// Optionally compress the data.
		if config.Compression {
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)

			_, err := zw.Write(data)
			exitOnErr(err)

			zw.Close()
			data = buf.Bytes()
		}

		err = zk.Set(paths[i], string(data))
		exitOnErr(err)
	}

	fmt.Println("\nData written to ZooKeeper")
}

func zkPaths(p string) []string {
	paths := []string{}

	var prefix string
	switch p {
	case "/":
		prefix = ""
	case "":
		prefix = ""
	default:
		prefix = fmt.Sprintf("/%s", p)
		paths = append(paths, prefix)
	}

	paths = append(paths, prefix+"/partitionmeta")
	paths = append(paths, prefix+"/brokermetrics")

	return paths
}

func createZNodesIfNotExist(z kafkazk.Handler, p []string) error {
	// Check each path.
	for _, path := range p {
		exist, err := z.Exists(path)
		if err != nil {
			return err
		}
		// Create it if it doesn't exist.
		if !exist {
			err := z.Create(path, "")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func exitOnErr(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
