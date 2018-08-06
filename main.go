package googlecloudpubsubhelper

import (
	"encoding/json"
	"golang.org/x/net/context"
	"io/ioutil"

	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
)

type messageHanlder func(*pubsub.Message)

// Google credentials file struct
type GoogleConfigJSON struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
}

var (
	googleConfig GoogleConfigJSON
	pubsubClient *pubsub.Client
	ctx          context.Context
)

func Configure(googleCredentialsPath string) {
	googleJSONKey, err := ioutil.ReadFile(googleCredentialsPath)
	if err != nil {
		log.Fatal("Couldn't read Google Credentials file:", err)
	}

	if err := json.Unmarshal(googleJSONKey, &googleConfig); err != nil {
		log.Fatal("Unable to unmarshal Google Credentials:", err)
	}

	log.WithFields(log.Fields{
		"clientEmail": googleConfig.ClientEmail,
	}).Info("google credentials OK.")

	ctx = context.Background()
	pubsubClient, err = pubsub.NewClient(ctx, googleConfig.ProjectID)
	if err != nil {
		log.Fatal("Unable to create pubsub client:", err)
	}
}

func Subscribe(subscriptionName string, handler messageHanlder) (cancel context.CancelFunc, err error) {
	sub := pubsubClient.Subscription(subscriptionName)
	cctx, cancel := context.WithCancel(ctx)
	logger := log.WithFields(log.Fields{
		"source":           "TestonPubSubClient",
		"subscriptionName": subscriptionName,
	})
	logger.Info("Listening for messages")
	return cancel, sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		// Pass to handler
		handler(msg)
	})
}

func Publish(topicName string, message *pubsub.Message) (err error) {
	topic := pubsubClient.Topic(topicName)
	_, err = topic.Exists(ctx)
	if err != nil {
		return err
	}

	result := topic.Publish(ctx, message)
	_, err = result.Get(ctx)
	return err
}
