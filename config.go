package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/opensourceways/kafka-lib/mq"
	"k8s.io/apimachinery/pkg/util/sets"
)

var reIpPort = regexp.MustCompile(`^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}:[1-9][0-9]*$`)

type configuration struct {
	Config accessConfig `json:"access,omitempty"`
}

func (c *configuration) Validate() error {
	return c.Config.validate()
}

func (c *configuration) SetDefault() {}

type accessConfig struct {
	// RepoPlugins list of plugins needed to configure the repository
	RepoPlugins map[string][]string `json:"repo_plugins,omitempty"`

	// Plugins is a list available plugins.
	Plugins []pluginConfig `json:"plugins,omitempty"`

	Broker string `json:"broker,omitempty"`
}

func (a accessConfig) validate() error {
	for i := range a.Plugins {
		if err := a.Plugins[i].validate(); err != nil {
			return err
		}
	}

	ps := make([]string, len(a.Plugins))
	for i := range a.Plugins {
		ps[i] = a.Plugins[i].Name
	}

	total := sets.NewString(ps...)

	for k, item := range a.RepoPlugins {
		if v := sets.NewString(item...).Difference(total); v.Len() != 0 {
			return fmt.Errorf(
				"%s: unknown plugins(%s) are set", k,
				strings.Join(v.UnsortedList(), ", "),
			)
		}
	}

	if r := a.parseAddress(); len(r) == 0 {
		return errors.New("invalid broker address")
	}

	return nil
}

type eventsDemux map[string][]string

func updateDemux(p *pluginConfig, d eventsDemux) {
	endpoint := p.Endpoint

	for _, e := range p.Events {
		if es, ok := d[e]; ok {
			d[e] = append(es, endpoint)
		} else {
			d[e] = []string{endpoint}
		}
	}
}

func orgOfRepo(repo string) string {
	spliter := "/"
	if strings.Contains(repo, spliter) {
		return strings.Split(repo, spliter)[0]
	}
	return ""
}

func (a accessConfig) getDemux() map[string]eventsDemux {
	plugins := make(map[string]int)
	for i := range a.Plugins {
		plugins[a.Plugins[i].Name] = i
	}

	r := make(map[string]eventsDemux)
	rp := a.RepoPlugins

	for k, ps := range rp {
		events, ok := r[k]
		if !ok {
			events = make(eventsDemux)
			r[k] = events
		}

		// inherit the config of org if k is a repo.
		if org := orgOfRepo(k); org != "" {
			ps = append(ps, rp[org]...)
		}

		for _, p := range ps {
			if i, ok := plugins[p]; ok {
				updateDemux(&a.Plugins[i], events)
			}
		}
	}

	return r
}

func (a accessConfig) mqConfig() mq.MQConfig {
	return mq.MQConfig{
		Addresses: a.parseAddress(),
	}
}

func (a accessConfig) parseAddress() []string {
	v := strings.Split(a.Broker, ",")
	r := make([]string, 0, len(v))
	for i := range v {
		if reIpPort.MatchString(v[i]) {
			r = append(r, v[i])
		}
	}

	return r
}

type pluginConfig struct {
	// Name of the service.
	Name string `json:"name" required:"true"`

	// Endpoint is the location of the service.
	Endpoint string `json:"endpoint" required:"true"`

	// Events are the events that this service can handle and should be forward to it.
	// If no events are specified, everything is sent.
	Events []string `json:"events,omitempty"`
}

func (p pluginConfig) validate() error {
	if p.Name == "" {
		return fmt.Errorf("missing name")
	}

	if p.Endpoint == "" {
		return fmt.Errorf("missing endpoint")
	}

	// TODO validate the value of p.Endpoint
	return nil
}
