package webapi

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/cenk/backoff"
	"github.com/containous/flaeg/parse"
	"github.com/containous/traefik/job"
	"github.com/containous/traefik/safe"
	"github.com/containous/traefik/types"
	"github.com/sirupsen/logrus"
)

// Provider holds configurations of the Provider provider.
type Provider struct {
	Endpoint      string         `description:"Comma separated server endpoints"`
	Cluster       string         `description:"Web cluster"`
	Watch         bool           `description:"Watch provider"`
	CheckInterval parse.Duration `description:"Check interval for config"`
	version       int
}

// Init the provider
func (p *Provider) Init(_ types.Constraints) error {
	return nil
}

// Provide allows the provider to provide configurations to traefik
// using the given configuration channel.
func (p *Provider) Provide(configurationChan chan<- types.ConfigMessage, pool *safe.Pool) error {
	logrus.Infof("webapi > {Endpoint: %s, Watch: %v, Cluster: %s, CheckInterval: %v}",
		p.Endpoint, p.Watch, p.Cluster, p.CheckInterval)

	safe.Go(func() {
		if p.Watch {
			pool.Go(func(stop chan bool) {
				d := time.Duration(p.CheckInterval)
				if d == 0 {
					d = 30 * time.Second
				}
				ticker := time.NewTicker(d)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						p.refresh(configurationChan)
					case <-stop:
						ticker.Stop()
						return
					}
				}
			})
		}

		operation := func() error {
			p.version = p.loadVersion()
			cfg, err := p.loadConfig()
			if err == nil {
				configurationChan <- cfg
			} else {
				logrus.Errorf("webapi > Failed to load configuration: %+v", err)
			}
			return err
		}
		notify := func(err error, time time.Duration) {
			logrus.Errorf("Provider connection error %+v, retrying in %s", err, time)
		}
		err := backoff.RetryNotify(safe.OperationWithRecover(operation), job.NewBackOff(backoff.NewExponentialBackOff()), notify)
		if err != nil {
			logrus.Errorf("Cannot connect to webapi provider: %+v", err)
		}
	})
	return nil
}

func (p *Provider) loadVersion() (version int) {
	data, err := p.request("/traefik/version")
	if err != nil {
		logrus.Errorf("webapi > load version failed: %v", err)
		return
	}

	v := struct {
		Version int `json:"version" bson:"-"`
	}{}
	err = json.Unmarshal(data, &v)
	if err != nil {
		logrus.Errorf("webapi > unmarshal verson failed: %v", err)
		return
	}

	return v.Version
}

func (p *Provider) loadConfig() (cfg types.ConfigMessage, err error) {
	var data []byte
	data, err = p.request("/traefik/config")
	if err != nil {
		return
	}

	cfg = types.ConfigMessage{
		ProviderName:  "webapi",
		Configuration: new(types.Configuration),
	}
	err = json.Unmarshal(data, &cfg.Configuration)
	if err != nil {
		logrus.Errorf("unmarshal config failed: %v", err)
	}
	return
}

func (p *Provider) refresh(configurationChan chan<- types.ConfigMessage) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("webapi > refresh panic: %v", r)
		}
	}()

	if version := p.loadVersion(); version != p.version {
		logrus.Infof("webapi > found new version: %v", version)
		cfg, err := p.loadConfig()
		if err == nil {
			p.version = version
			configurationChan <- cfg
		} else {
			logrus.Errorf("webapi > update failed: %v", err)
		}
	}
}

func (p *Provider) request(path string) (data []byte, err error) {
	servers := strings.Split(p.Endpoint, ",")
	if len(servers) == 0 {
		err = errors.New("webapi > endpoint must be configured")
		return
	}

	for _, server := range servers {
		data, err = p.requestServer(path, server)
		if err == nil {
			return
		}
	}
	return
}

func (p *Provider) requestServer(path, server string) (data []byte, err error) {
	u := server + path
	if p.Cluster != "" {
		u = u + "?cluster=" + p.Cluster
	}

	var resp *http.Response
	client := http.Client{Timeout: time.Second * 5}
	resp, err = client.Get(u)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New(string(data))
		return
	}

	return data, nil
}
