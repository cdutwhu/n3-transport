// n3influx_publisher.go

package n3influx

import (
	"log"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/nsip/n3-transport/pb"
)

type Publisher struct {
	cl influx.Client
	ch chan *influx.Point
}

func NewPublisher() (*Publisher, error) {

	n3ic := &Publisher{
		ch: make(chan *influx.Point),
	}

	ifclient, err := influxClient()
	if err != nil {
		return nil, err
	}
	n3ic.cl = ifclient
	log.Println("starting storeage handler...")
	go n3ic.startStorageHandler()
	return n3ic, nil
}

func influxClient() (influx.Client, error) {

	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: "http://localhost:8086",
		// Username: username,
		// Password: password,
	})
	if err != nil {
		log.Println("Cannot connect to influx server")
		return nil, err
	}
	return c, nil

}

//
// influx db inserts are best done as batches so this
// handler listens on the storage channel, and sends inserts to
// influx when a batch size has been reached or a timing threshold
// is passed
//
func (n3ic *Publisher) startStorageHandler() {

	var coll []*influx.Point
	batchInterval := time.Duration(time.Millisecond * 500)
	batchSize := 500
	tick := time.NewTicker(batchInterval)

	for {
		timeout := false

		select {
		case pt := <-n3ic.ch:
			coll = append(coll, pt)
		case <-tick.C:
			timeout = true
		}

		if (timeout || len(coll) >= batchSize) && len(coll) > 0 {
			bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
				Database: "tuples",
				// Precision: "s",
			})
			if err != nil {
				//TODO:
				log.Println(err)
			}
			bp.AddPoints(coll)
			err = n3ic.cl.Write(bp)
			if err != nil {
				//TODO:
				log.Println(err)
			} else {
				coll = nil
			}
		}
	}
}

//
// send the tuple to influx, passes into batching storage handler
//
func (n3ic *Publisher) StoreTuple(tuple *pb.SPOTuple) error {

	// extract data from tuple and use to construct point
	tags := map[string]string{
		"subject":   tuple.Subject,
		"predicate": tuple.Predicate,
		"object":    tuple.Object,
	}
	fields := map[string]interface{}{
		"version": tuple.Version,
		// "predicate": tuple.Predicate,
		// "object":    tuple.Object,
	}

	pt, err := influx.NewPoint(tuple.Context, tags, fields, time.Now())
	if err != nil {
		return err
	}

	n3ic.ch <- pt

	return nil
}

//
//
//
func spacer() {}

//
//
//