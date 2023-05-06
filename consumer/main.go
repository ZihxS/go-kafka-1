package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func run() {
	hostname, _ := os.Hostname()

	kafkaConfigMap := &kafka.ConfigMap{
		"bootstrap.servers":    "localhost",
		"group.id":             "go-kafka-1",
		"client.id":            hostname,
		"auto.offset.reset":    "earliest",
		"max.poll.interval.ms": 300000 * 10,
	}

	consumer, err := kafka.NewConsumer(kafkaConfigMap)

	if err != nil {
		panic(err)
	}

	topics := []string{
		"go-kafka-1-topic-1",
		"go-kafka-1-topic-2",
		"go-kafka-1-topic-3",
		"go-kafka-1-topic-4",
		"go-kafka-1-topic-5",
	}

	consumer.SubscribeTopics(topics, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func main() {
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go run()

	log.Printf("Waiting For Messages. To Exit Please Press CTRL+C.\n")
	<-exitChan

	fmt.Println("")
	log.Println("Shutdown Signal Received!")
	log.Println("Bye Bye!")
}
