package main

import (
	"context"
	"math/rand"
	"os/signal"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"flag"
	"fmt"
	"os"
)

func main() {
	topicPtr := flag.String("t", "", "The ARN of the topic to which the user subscribes")
	sleepPtr := flag.Int64("s", 3, "The number of seconds to sleep between enqueues")

	flag.Parse()

	if *topicPtr == "" {
		fmt.Println("You must supply a topic ARN")
		fmt.Println("Usage: go run main.go -t TOPIC_ARN")
		os.Exit(1)
	}

	if *sleepPtr < 1 {
		*sleepPtr = 1
	}

	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file. (~/.aws/credentials).
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sns.New(sess)

	exitCh := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())

	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	go func(ctx context.Context) {
		fmt.Println("Starting loop. Press ^C to stop.")
		for {
			select {
			case <-ctx.Done():
				exitCh <- true
				return
			default:
				version := rng.Intn(2) + 1
				_, err := svc.Publish(&sns.PublishInput{
					Message:  aws.String(fmt.Sprintf("Message to version %d.", version)),
					TopicArn: topicPtr,
					MessageAttributes: map[string]*sns.MessageAttributeValue{
						"version": {
							DataType:    aws.String("Number"),
							StringValue: aws.String(strconv.Itoa(version)),
						},
					},
				})
				if err != nil {
					fmt.Println(err.Error())
					os.Exit(1)
				}
				fmt.Printf("Enqueued message for version %d.\n", version)
				time.Sleep(time.Second * time.Duration(*sleepPtr))
			}
		}
	}(ctx)

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		select {
		case <-sigCh:
			fmt.Println("Received signal. Exiting.")
			cancel()
			return
		}
	}()
	<-exitCh
}
