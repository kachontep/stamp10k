package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kachontep/stamp10k/schema"
)

var reqQueue = make(chan *schema.BookingRequest)
var reqMap = make(map[string]*synchronousBookingRequest)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <request_topic> <response_topic> <group>\n",
			os.Args[0])
		os.Exit(1)
	}
	broker := os.Args[1]
	requestTopic := os.Args[2]
	responseTopic := os.Args[3]
	group := os.Args[4]
	producerService(broker, requestTopic, reqQueue)
	consumerService(broker, responseTopic, group, reqMap)
	httpService()
}

func consumerService(broker, topic, group string, requestMap map[string]*synchronousBookingRequest) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": false,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": false,
		"auto.offset.reset":    "earliest",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	go func() {
		run := true
		for run == true {
			select {
			case ev := <-c.Events():
				switch e := ev.(type) {
				case *kafka.Message:
					fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
					processBookingResponse(e, requestMap)
				case kafka.Error:
					// Errors should generally be considered as informational, the client will try to automatically recover
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				}
			}
		}
		fmt.Printf("Closing consumer\n")
		c.Close()
	}()
}

func producerService(broker, topic string, requestChannel <-chan *schema.BookingRequest) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"boostrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Create Producer %v\n", p)

	// Process returned request message sending result
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	// Process waiting message in request queue
	go func() {
		for reqMsg := range requestChannel {
			value, err := json.Marshal(reqMsg)
			if err != nil {
				fmt.Printf("Error marshalng message %v\n", reqMsg)
				continue
			}
			p.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value: value,
			}
		}
	}()
}

func processBookingResponse(e *kafka.Message, requestMap map[string]*synchronousBookingRequest) {
	var br schema.BookingResponse
	if err := json.Unmarshal(e.Value, &br); err != nil {
		fmt.Printf("Error unmarshaling message from topic %s [%d] %v\n",
			*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
		return
	}
	id := br.RequestID
	r, ok := requestMap[id]
	if !ok {
		fmt.Printf("Error request id %s not found\n", id)
		return
	}
	r.C <- &br
}

// Provide REST service for booking and current remaining stamps
func httpService() {

	// booking url: /booking POST  submit booking request to booking system
	http.HandleFunc("/booking", submitBooking)

	// stamps  url: /stamps  GET   get remaining stamps in system
	// http.HandleFunc("/checkStamps", checkStamps)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func submitBooking(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Validate request message
	var b schema.BookingRequest
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Send message asynchronously
	sr := newSyncBookingRequest(&b)

	// Wait for reply and reply to client

	// Hard limit timeout - this should be more than system regulator's timeout.
	timeout := time.NewTimer(5 * time.Second)

	select {
	case m := <-sr.C:
		if m.Error != "" {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"error": m.Error,
			})
			return
		}
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"bookingId": m.BookingID,
		})
	case <-timeout.C:
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Internal server processing timed out",
		})
	}
}

func checkStamps(w http.ResponseWriter, r *http.Request) {
	// TODO: Open for discussion in room
}

func newSyncBookingRequest(msg *schema.BookingRequest) *synchronousBookingRequest {
	// Make request id unique for each request
	msg.RequestID = makeUniqueID("booking")
	sr := synchronousBookingRequest{
		msg: msg,
		C:   make(chan *schema.BookingResponse, 1),
	}
	id := sr.msg.RequestID
	reqMap[id] = &sr
	reqQueue <- sr.msg
	return &sr
}

func makeUniqueID(ns string) string {
	t := time.Now()
	ts := fmt.Sprintf("%d%02d%02dT%02d%02d%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	r := rand.Intn(100000)
	return fmt.Sprintf("%s%s%d", ns, ts, r)
}

type synchronousBookingRequest struct {
	msg *schema.BookingRequest
	C   chan *schema.BookingResponse
}
