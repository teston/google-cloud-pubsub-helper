package main

import (
	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
	testonpubsub "github.com/teston/google-cloud-pubsub-helper"
)

func main() {
	logger := log.WithFields(log.Fields{"source": "pubsubSubscription"})
	pubsubHelper := testonpubsub.PubSubHelper{
		GoogleCredentialsPath: "/go/src/github.com/teston/google-cloud-pubsub-helper/google-credentials.json",
	}

	pubsubHelper.Configure()

	unsubscribe, err := pubsubHelper.Subscribe(
		"name-of-subscription",
		5, // max outstanding messages
		func(msg *pubsub.Message) {
			// Callback to handle a message
			msg.Ack()
			logger.Info("Got message:", string(msg.Data))
		},
	)

	if err != nil {
		unsubscribe()
		logger.Fatal("Message subscription cancelled", err)
	}
}
