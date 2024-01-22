package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"flag"
	"fmt"
)

func main() {
	queue1 := flag.String("q1", "", "The name of the first queue")
	queue2 := flag.String("q2", "", "The name of the second queue")
	timeout := flag.Int64("t", 5, "How long, in seconds, that the message is hidden from others")
	interval := flag.Int64("i", 1, "How long, in seconds, to wait between dequeues")
	flag.Parse()

	if *queue1 == "" || *queue2 == "" {
		fmt.Println("You must supply the name of the queues (-q1 QUEUE_ONE -q2 QUEUE_TWO)")
		return
	}

	if *timeout < 0 {
		*timeout = 0
	}

	if *timeout > 12*60*60 {
		*timeout = 12 * 60 * 60
	}

	if *interval < 1 {
		*interval = 1
	}

	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file. (~/.aws/credentials).
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	var urlResults []*sqs.GetQueueUrlOutput
	for _, queue := range []*string{queue1, queue2} {
		urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: queue,
		})
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		urlResults = append(urlResults, urlResult)
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}

	// fmt.Println(*urlResults[0].QueueUrl)
	// fmt.Println(*urlResults[1].QueueUrl)

	wg.Add(1)
	go dequeue(ctx, svc, urlResults[0].QueueUrl, timeout, &wg, interval)

	wg.Add(1)
	go dequeue(ctx, svc, urlResults[1].QueueUrl, timeout, &wg, interval)

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	fmt.Println("Received signal. Exiting.")
	cancel()
	wg.Wait()
}

func dequeue(ctx context.Context, svc *sqs.SQS, queueURL *string, timeout *int64, wg *sync.WaitGroup, interval *int64) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping dequeue.")
			return
		default:
			msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
				// AttributeNames: []*string{
				// 	aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
				// },
				// MessageAttributeNames: []*string{
				// 	aws.String(sqs.QueueAttributeNameAll),
				// },
				QueueUrl:            queueURL,
				MaxNumberOfMessages: aws.Int64(1),
				VisibilityTimeout:   timeout,
			})
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			if msgResult.Messages == nil || len(msgResult.Messages) == 0 {
				time.Sleep(time.Second * time.Duration(*interval))
				continue
			}

			messageHandle := msgResult.Messages[0].ReceiptHandle
			body := msgResult.Messages[0].Body

			fmt.Println(*queueURL + ": " + *body)

			_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: messageHandle,
			})
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			time.Sleep(time.Second * time.Duration(*interval))
		}
	}
}
