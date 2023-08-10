package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/server-common-lib/config"
	"github.com/opensourceways/server-common-lib/interrupts"
	"github.com/opensourceways/server-common-lib/logrusutil"
	liboptions "github.com/opensourceways/server-common-lib/options"
	"github.com/opensourceways/server-common-lib/utils"
	"github.com/sirupsen/logrus"
)

const component = "robot-github-hook-dispatcher"

type options struct {
	service liboptions.ServiceOptions
	topic   string
}

func (o *options) Validate() error {
	if o.topic == "" {
		return fmt.Errorf("please set topic")
	}

	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.StringVar(&o.topic, "topic", "", "The topic to which github webhook messages need to be published ")

	_ = fs.Parse(args)

	return o
}

func main() {
	logrusutil.ComponentInit(component)

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err := o.Validate(); err != nil {
		logrus.WithError(err).Fatal("Invalid options")
	}

	configAgent := config.NewConfigAgent(func() config.Config {
		return new(configuration)
	})
	if err := configAgent.Start(o.service.ConfigFile); err != nil {
		logrus.WithError(err).Fatal("Error starting config agent.")
	}

	agent := demuxConfigAgent{agent: &configAgent, t: utils.NewTimer()}
	agent.start()

	d := dispatcher{
		agent: &agent,
	}

	if err := initMQ(&configAgent); err != nil {
		logrus.WithError(err).Fatal("Error init broker.")
	}

	defer kafka.Disconnect()

	subscriber, err := kafka.Subscribe(o.topic, component, handleGiteeMessage(&d))
	if err != nil {
		logrus.WithError(err).Fatal(fmt.Sprintf("error subscribe %s topic.", o.topic))
	}

	defer subscriber.Unsubscribe()

	defer interrupts.WaitForGracefulShutdown()

	interrupts.OnInterrupt(func() {
		// agent depends on configAgent, so stop agent first.
		agent.stop()
		logrus.Info("demux stopped")

		configAgent.Stop()
		logrus.Info("config agent stopped")

		d.wait()
	})

	// Return 200 on / for health checks.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})

	httpServer := &http.Server{Addr: ":" + strconv.Itoa(o.service.Port)}

	interrupts.ListenAndServe(httpServer, o.service.GracePeriod)
}
