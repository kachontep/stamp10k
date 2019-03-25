package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kachontep/stamp10k/schema"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <src_topic> <sink_topic> <group>\n", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	srcTopic := os.Args[2]
	snkTopic := os.Args[3]
	group := os.Args[4]

	src := make(chan *kafka.Message)
	snk := make(chan *kafka.Message)

	producerClosed := make(chan bool, 1)
	producerDone, err := newProducer(broker, snk, producerClosed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Exit application since producer creation failed\n")
		os.Exit(1)
	}

	consumerClosed := make(chan bool, 1)
	consumerDone, err := newConsumer(broker, srcTopic, group, src, consumerClosed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Exit application since consumer creation failed\n")
		os.Exit(1)
	}

	// Support shutdown by interrupt and terminate signal
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigs:
			consumerClosed <- true
			<-consumerDone

			producerClosed <- true
			<-producerDone

			fmt.Printf("Interrupt to exit application\n")
			os.Exit(0)
		}
	}()

	// Initial state for processor
	var state map[string]interface{}
	state = make(map[string]interface{})
	state["count"] = int(0)
	state["quota"] = int(10)

	// For non-query process, the main go routine is used
	// to process the message. Use new go routine to
	// process this if there is a need for query in processor
	process(srcTopic, src, snkTopic, snk, state)
}

func newConsumer(broker, topic, group string, src chan<- *kafka.Message, closed <-chan bool) (done chan bool, err error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": false,
		"enable.partition.eof":            false,
		"auto.offset.reset":               "earliest",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		return
	}

	fmt.Printf("Create Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe topic: %s\n", topic)
		return
	}

	done = make(chan bool, 1)
	go func() {
		run := true
		for run {
			select {
			case <-closed:
				run = false
			case ev := <-c.Events():
				switch e := ev.(type) {
				case *kafka.Message:
					src <- e
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				}
			}
		}
		fmt.Println("Close consumer")
		c.Close()
		done <- true
	}()
	return
}

func newProducer(broker string, snk <-chan *kafka.Message, closed <-chan bool) (done chan bool, err error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}

	fmt.Printf("Create Producer %v\n", p)

	send := func(o *kafka.Message, delivered chan kafka.Event) {
		p.Produce(o, delivered)
		e := <-delivered
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}

	done = make(chan bool, 1)
	go func() {
		delivered := make(chan kafka.Event)
		run := true
		for run {
			select {
			case <-closed:
				run = false
			case o := <-snk:
				send(o, delivered)
			}
		}
		close(delivered)
		fmt.Printf("Closing producer\n")
		p.Close()
		done <- true
	}()
	return
}

func process(srcTopic string, src chan *kafka.Message, snkTopic string, snk chan *kafka.Message, initState map[string]interface{}) {
	count := initState["count"].(int)
	quota := initState["quota"].(int)

	fmt.Printf("Initialize processor state {count=%d, quota=%d}\n", count, quota)

	for e := range src {
		var brq schema.BookingRequest

		err := json.Unmarshal(e.Value, &brq)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while processing: %s\n%s", e.TopicPartition, string(e.Value))
			continue
		}

		// Check stock before deciding result
		var brs *schema.BookingResponse
		if count < quota {
			// Reduce stock after sending message success
			// FIX: it should reduce count after success reponse from sink receieved
			count++
			bookingID := fmt.Sprintf("stamp%07d", count)
			brs = &schema.BookingResponse{
				RequestID: brq.RequestID,
				BookingID: bookingID,
			}
		} else {
			brs = &schema.BookingResponse{
				RequestID: brq.RequestID,
				Error:     "Stamp is sold out",
			}
		}

		value, err := json.Marshal(brs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while marshal output: %v\n", brs)
			continue
		}

		snk <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &snkTopic,
				Partition: kafka.PartitionAny,
			},
			Value: value,
		}
	}
}
