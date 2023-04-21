package main

import (
	"errors"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
	"github.com/opensourceways/server-common-lib/config"
	"github.com/sirupsen/logrus"
)

func initMQ(agent config.ConfigAgent) error {
	cfg := &configuration{}
	_, c := agent.GetConfig()

	if v, ok := c.(*configuration); ok {
		cfg = v
	}

	tlsConfig, err := cfg.Config.Broker.TLSConfig.TLSConfig()
	if err != nil {
		return err
	}

	err = kafka.Init(
		mq.Addresses(cfg.Config.Broker.Addresses...),
		mq.SetTLSConfig(tlsConfig),
		mq.Log(logrus.WithField("module", "broker")),
		mq.ErrorHandler(errorHandler()),
	)

	if err != nil {
		return err
	}

	return kafka.Connect()
}

func handleGiteeMessage(d *dispatcher) mq.Handler {
	return func(event mq.Event) error {
		return d.HandlerMsg(event)
	}
}

func parseWebHookInfoFromMsg(msg *mq.Message) (eventType, uuid string, payload []byte, err error) {
	if msg == nil {
		err = errors.New("get a nil msg from broker")
		return
	}

	if ua := msg.Header["User-Agent"]; ua != "Robot-Github-Access" {
		err = errors.New("unexpect github message: Missing User-Agent Header")

		return
	}

	if eventType = msg.Header["X-GitHub-Event"]; eventType == "" {
		err = errors.New("unexpect github message: Missing X-Gitee-Event Header")

		return
	}

	if uuid = msg.Header["X-GitHub-Delivery"]; uuid == "" {
		err = errors.New("unexpect github message: Missing X-Gitee-Timestamp Header")

		return
	}

	if payload = msg.Body; len(payload) == 0 {
		err = errors.New("unexpect github message: The payload is empty")
	}

	return
}

func errorHandler() mq.Handler {
	return func(event mq.Event) error {
		l := logrus.WithFields(logrus.Fields{
			"msg error handle": "default handler",
		})

		l.Errorf(
			"the %s message handler occur error: %v, extra info that: %v",
			event.Message().MessageKey(),
			event.Error(),
			event.Extra(),
		)

		return nil
	}
}
