package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// FIXME Move to utils.go
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s\n", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	// FIXME Same queue as the one in the publisher. Best practice to declare it in a separate file to ensure consistency? YES, fix it later, move it to config.go
	q, err := ch.QueueDeclare(
		"hello", //name
		false,   //durable
		false,   //delete when unused
		false,   //exclusive
		false,   // no-wait
		nil,     //arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	// TODO What is chan? Channels
	// - Channels are fundamental structures in goroutines
	// - Pipes for sync data trasmission
	var forever chan struct{}

	// Since messages are push async, read is done in a go routine
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever // TODO What is this notation? Consume values from a channel. If it was forever <- value it would be sending a value to the channel (producing)
}
