package googlecloudpubsubhelper

import (
	"encoding/json"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"

	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
)

type messageHanlder func(*pubsub.Message)

// Google credentials file struct
type googleConfigJSON struct {
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

type PubSubHelper struct {
	GoogleCredentialsPath string
	GoogleConfig          googleConfigJSON
	Client                *pubsub.Client
	Ctx                   context.Context
}

// var (
// 	googleConfig googleConfigJSON
// 	pubsubClient *pubsub.Client
// 	ctx          context.Context
// )

func (p *PubSubHelper) Configure() {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", p.GoogleCredentialsPath)

	googleJSONKey, err := ioutil.ReadFile(p.GoogleCredentialsPath)
	if err != nil {
		log.Fatal("Couldn't read Google Credentials file:", err)
	}

	if err := json.Unmarshal(googleJSONKey, &p.GoogleConfig); err != nil {
		log.Fatal("Unable to unmarshal Google Credentials:", err)
	}

	log.WithFields(log.Fields{
		"clientEmail": p.GoogleConfig.ClientEmail,
	}).Info("google credentials OK.")

	p.Ctx = context.Background()
	p.Client, err = pubsub.NewClient(p.Ctx, p.GoogleConfig.ProjectID)
	if err != nil {
		log.Fatal("Unable to create pubsub client:", err)
	}
}

func (p *PubSubHelper) Subscribe(subscriptionName string, maxOutstandingMessages int, handler messageHanlder) (
	cancel context.CancelFunc,
	err error) {
	sub := p.Client.Subscription(subscriptionName)
	sub.ReceiveSettings.MaxOutstandingMessages = maxOutstandingMessages

	cctx, cancel := context.WithCancel(p.Ctx)

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

func (p *PubSubHelper) Publish(topicName string, message *pubsub.Message) (err error) {
	topic := p.Client.Topic(topicName)
	_, err = topic.Exists(p.Ctx)
	if err != nil {
		return err
	}

	result := topic.Publish(p.Ctx, message)
	_, err = result.Get(p.Ctx)
	return err
}
