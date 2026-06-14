// Package consul provides a reusable Consul KV client with profile-based YAML merge.
// Mirrors b2b_common.utils.consul_config in b2b-common-py.
package consul

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v3"
)

const BaseProfileFolder = "application"

// ClientOptions configures the Consul KV client.
type ClientOptions struct {
	// Hostname is resolved via DNS/hosts first (default consul.mmt.mmt).
	Hostname string
	// FallbackHost is used when Hostname does not resolve.
	FallbackHost string
	Port         int
	Token        string
}

// Client fetches and merges YAML configuration from Consul KV.
type Client struct {
	host  string
	port  int
	token string

	mu     sync.Mutex
	apiCli *api.Client
}

// NewClient builds a Consul client. Host resolution matches b2b-common:
// resolve Hostname from /etc/hosts or DNS, else use FallbackHost.
func NewClient(opts ClientOptions) (*Client, error) {
	host := opts.Hostname
	if host == "" {
		host = "consul.mmt.mmt"
	}
	resolved := ResolveHost(host)
	if resolved == "" {
		resolved = opts.FallbackHost
	}
	if resolved == "" {
		return nil, fmt.Errorf("consul: unable to resolve host %q and no fallback host configured", host)
	}

	port := opts.Port
	if port == 0 {
		port = 8500
	}

	token := opts.Token
	if token == "" {
		token = os.Getenv("CONSUL_TOKEN")
	}

	return &Client{host: resolved, port: port, token: token}, nil
}

// ResolveHost returns an IP/hostname suitable for dialing Consul.
func ResolveHost(hostname string) string {
	if ip := net.ParseIP(hostname); ip != nil {
		return hostname
	}
	ips, err := net.LookupHost(hostname)
	if err != nil || len(ips) == 0 {
		return ""
	}
	return ips[0]
}

func (c *Client) apiClient() (*api.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.apiCli != nil {
		return c.apiCli, nil
	}
	cfg := api.DefaultConfig()
	cfg.Address = fmt.Sprintf("%s:%d", c.host, c.port)
	if c.token != "" {
		cfg.Token = c.token
	}
	cli, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	c.apiCli = cli
	return c.apiCli, nil
}

// FetchKeyValue loads and parses YAML for a single Consul KV key.
func (c *Client) FetchKeyValue(key string) (map[string]any, error) {
	raw, err := c.fetchRaw(key)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var parsed map[string]any
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("consul: parse %q: %w", key, err)
	}
	return parsed, nil
}

// Fetch loads key templates with profile merge.
// Each template must contain a single %s placeholder for the profile folder
// (e.g. "Platform/MMT-AI/GROUNDS-MCP/mcp/%s/props.yml").
// PROFILE env, when set, merges application,<PROFILE> over application.
func (c *Client) Fetch(keyTemplates []string) (map[string]map[string]any, error) {
	profile := os.Getenv("PROFILE")
	result := make(map[string]map[string]any, len(keyTemplates))

	for _, template := range keyTemplates {
		baseKey := fmt.Sprintf(template, BaseProfileFolder)
		baseCfg, err := c.FetchKeyValue(baseKey)
		if err != nil {
			return nil, err
		}
		if baseCfg == nil {
			baseCfg = map[string]any{}
		}

		if profile == "" {
			result[template] = baseCfg
			continue
		}

		profileKey := fmt.Sprintf(template, BaseProfileFolder+","+profile)
		profileCfg, err := c.FetchKeyValue(profileKey)
		if err != nil {
			return nil, err
		}
		if profileCfg == nil {
			result[template] = baseCfg
			continue
		}
		result[template] = DeepMergeMaps(baseCfg, profileCfg)
	}

	return result, nil
}

// FetchMergedYAML returns merged YAML bytes for one key template (profile-aware).
func (c *Client) FetchMergedYAML(keyTemplate string) ([]byte, error) {
	data, err := c.Fetch([]string{keyTemplate})
	if err != nil {
		return nil, err
	}
	merged, ok := data[keyTemplate]
	if !ok || len(merged) == 0 {
		return nil, nil
	}
	return yaml.Marshal(merged)
}

func (c *Client) fetchRaw(key string) ([]byte, error) {
	cli, err := c.apiClient()
	if err != nil {
		return nil, err
	}
	pair, _, err := cli.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}
	if pair == nil || len(pair.Value) == 0 {
		return nil, nil
	}
	return pair.Value, nil
}
