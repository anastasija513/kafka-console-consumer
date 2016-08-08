package main

import (
	"github.com/Shopify/sarama"

	"log"
	"flag"
	"os"
	"sync"
	"strconv"
	"strings"
)

var (
	topic       = flag.String("topic", "", "reading topic")
	brokers     = flag.String("brokers", "", "list of brokers")
	partitions  = flag.String("partitions", "", "list of partitions")
)


func main() {

	flag.Parse()
	checkDefault(*topic)
	checkDefault(*brokers)
	checkDefault(*partitions)

	partitions := strings.Fields(*partitions)
	brokers  := strings.Fields(*brokers)

	//brokers  := []string{"localhost:9092"}
	//partitions := []string{"0", "2"}
	//topic := "inputTopic201"

	client := buildClient(brokers)
	defer client.Close()

	consumer := buildConsumer(client)
	defer consumer.Close()

	var wg sync.WaitGroup

	for _, p := range partitions {
		wg.Add(1)
		partId, _ := strconv.ParseInt(p, 10, 0)

		go func() {
			defer wg.Done()

			consumerPart := buildConsumer(client)
			defer consumerPart.Close()

			c, err := consumerPart.ConsumePartition(topic, int32(partId), 0)
			if err != nil {
				log.Printf("Consumer: всё пропало!")
				panic(err)
			}

			offsetNewest, err := client.GetOffset(topic, int32(partId), sarama.OffsetNewest)
			if err != nil {
				log.Printf("Get offset newest failed")
				panic(err)
			}

				for msg := range c.Messages() {
					msgKey := sarama.StringEncoder(msg.Key)
					msgVal := sarama.ByteEncoder(msg.Value)
					msgOffset := msg.Offset

					log.Println("partId", partId, "msgKey", msgKey, "msgVal", msgVal)

					if (offsetNewest == (msgOffset + 1)) {
						log.Println("Read partition", partId)
						break
					}
			}
		}()
	}

	wg.Wait()
}

func buildClient(brokerList []string) sarama.Client {

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		panic(err)
	} else {
		log.Println("Connected.")
	}

	return client
}

func buildConsumer(client sarama.Client) sarama.Consumer {

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalln("Consumer: я не хочу ничего решать", err)
	}

	return consumer
}

func checkDefault(value string) {
	if value == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
}
