package promtailconvert

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/alecthomas/units"
	"github.com/grafana/agent/component/common/loki"
	lokiwrite "github.com/grafana/agent/component/loki/write"
	"github.com/grafana/agent/converter/diag"
	"github.com/grafana/agent/converter/internal/common"
	"github.com/grafana/agent/converter/internal/prometheusconvert"
	"github.com/grafana/agent/converter/internal/promtailconvert/internal/build"
	"github.com/grafana/agent/pkg/river/token/builder"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/clients/pkg/promtail/client"
	promtailcfg "github.com/grafana/loki/clients/pkg/promtail/config"
	"github.com/grafana/loki/clients/pkg/promtail/limit"
	"github.com/grafana/loki/clients/pkg/promtail/positions"
	"github.com/grafana/loki/clients/pkg/promtail/scrapeconfig"
	"github.com/grafana/loki/clients/pkg/promtail/targets/file"
	lokicfgutil "github.com/grafana/loki/pkg/util/cfg"
	lokiflag "github.com/grafana/loki/pkg/util/flagext"
	"gopkg.in/yaml.v2"
)

type Config struct {
	promtailcfg.Config `yaml:",inline"`
}

// Clone takes advantage of pass-by-value semantics to return a distinct *Config.
// This is primarily used to parse a different flag set without mutating the original *Config.
func (c *Config) Clone() flagext.Registerer {
	return func(c Config) *Config {
		return &c
	}(*c)
}

// Convert implements a Promtail config converter.
func Convert(in []byte) ([]byte, diag.Diagnostics) {
	var (
		diags diag.Diagnostics
		cfg   Config
	)

	// Set default values first.
	flagSet := flag.NewFlagSet("", flag.PanicOnError)
	err := lokicfgutil.Unmarshal(&cfg,
		lokicfgutil.Defaults(flagSet),
	)
	if err != nil {
		diags.Add(diag.SeverityLevelError, fmt.Sprintf("failed to set default Promtail config values: %s", err))
		return nil, diags
	}

	// Unmarshall explicitly specified values
	if err := yaml.UnmarshalStrict(in, &cfg); err != nil {
		diags.Add(diag.SeverityLevelError, fmt.Sprintf("failed to parse Promtail config: %s", err))
		return nil, diags
	}

	// Replicate promtails' handling of this deprecated field.
	if cfg.ClientConfig.URL.URL != nil {
		// if a single client config is used we add it to the multiple client config for backward compatibility
		cfg.ClientConfigs = append(cfg.ClientConfigs, cfg.ClientConfig)
	}

	f := builder.NewFile()
	diags = AppendAll(f, &cfg.Config, diags)

	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		diags.Add(diag.SeverityLevelError, fmt.Sprintf("failed to render Flow config: %s", err.Error()))
		return nil, diags
	}

	if len(buf.Bytes()) == 0 {
		return nil, diags
	}

	prettyByte, newDiags := common.PrettyPrint(buf.Bytes())
	diags = append(diags, newDiags...)
	return prettyByte, diags
}

// AppendAll analyzes the entire promtail config in memory and transforms it
// into Flow components. It then appends each argument to the file builder.
func AppendAll(f *builder.File, cfg *promtailcfg.Config, diags diag.Diagnostics) diag.Diagnostics {
	// We currently do not support the new global file watch config. It's an error, since setting it indicates
	// some advanced tuning which the user likely needs.
	if cfg.Global.FileWatch != file.DefaultWatchConig {
		diags.Add(diag.SeverityLevelError, "global/file_watch_config is not supported")
	}

	// The positions global config is not supported in Flow Mode. It's a warning since the Flow Mode likely
	// provides a good enough alternative through components' positions files in its own data directory.
	if cfg.PositionsConfig != defaultPositionsConfig() {
		diags.Add(
			diag.SeverityLevelWarn,
			"positions configuration is not supported - each Flow Mode's loki.source.file component "+
				"has its own positions file in the component's data directory",
		)
	}

	// The global and per-client stream lag labels is deprecated and has no effect.
	if len(cfg.Options.StreamLagLabels) > 0 {
		diags.Add(
			diag.SeverityLevelWarn,
			"stream_lag_labels is deprecated and the associated metric has been removed",
		)
	}

	// WAL support is still work in progress and not documented. Enabling it won't work, so it's an error.
	if cfg.WAL.Enabled {
		diags.Add(
			diag.SeverityLevelError,
			"Promtail's WAL is currently not supported in Flow Mode",
		)
	}

	// Not yet supported, see https://github.com/grafana/agent/issues/4342. It's an error since we want to
	// err on the safe side.
	//TODO(thampiotr): seems like it's possible to support this using loki.process component
	if cfg.LimitsConfig != defaultLimitsConfig() {
		diags.Add(
			diag.SeverityLevelError,
			"limits_config is not yet supported in Flow Mode",
		)
	}

	// We cannot migrate the tracing config to Flow Mode, since in promtail it relies on
	// environment variables that can be set or not and depending on what is set, different
	// features of tracing are configured. We'd need to have conditionals in the
	// flow config to translate this. See https://www.jaegertracing.io/docs/1.16/client-features/
	if cfg.Tracing.Enabled {
		diags.Add(
			diag.SeverityLevelWarn,
			"tracing configuration cannot be migrated to Flow Mode automatically - please "+
				"refer to documentation on how to configure tracing in Flow Mode",
		)
	}

	if cfg.TargetConfig.Stdin {
		diags.Add(
			diag.SeverityLevelError,
			"reading targets from stdin is not supported in Flow Mode configuration file",
		)
	}

	var writeReceivers = make([]loki.LogsReceiver, len(cfg.ClientConfigs))
	// Each client config needs to be a separate remote_write,
	// because they may have different ExternalLabels fields.
	for i, cc := range cfg.ClientConfigs {
		writeReceivers[i] = appendLokiWrite(f, &cc, &diags, i)
	}

	gc := &build.GlobalContext{
		WriteReceivers:   writeReceivers,
		TargetSyncPeriod: cfg.TargetConfig.SyncPeriod,
	}

	for _, sc := range cfg.ScrapeConfig {
		appendScrapeConfig(f, &sc, &diags, gc)
	}

	return diags
}

func defaultPositionsConfig() positions.Config {
	// We obtain the default by registering the flags
	cfg := positions.Config{}
	cfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	return cfg
}

func defaultLimitsConfig() limit.Config {
	cfg := limit.Config{}
	cfg.RegisterFlagsWithPrefix("", flag.NewFlagSet("", flag.PanicOnError))
	return cfg
}

func appendScrapeConfig(
	f *builder.File,
	cfg *scrapeconfig.Config,
	diags *diag.Diagnostics,
	gctx *build.GlobalContext,
) {
	b := build.NewScrapeConfigBuilder(f, diags, cfg, gctx)

	// Append all the SD components
	b.AppendKubernetesSDs()
	//TODO(thampiotr): add support for other SDs

	// Append loki.source.file to process all SD components' targets.
	// If any relabelling is required, it will be done via a discovery.relabel component.
	// The files will be watched and the globs in file paths will be expanded using discovery.file component.
	// The log entries are sent to loki.process if processing is needed, or directly to loki.write components.
	b.AppendLokiSourceFile()

	// Append all the components that produce logs directly.
	// If any relabelling is required, it will be done via a loki.relabel component.
	// The logs are sent to loki.process if processing is needed, or directly to loki.write components.
	b.AppendCloudFlareConfig()
	b.AppendJournalConfig()
	//TODO(thampiotr): add support for integrations
}

func appendLokiWrite(f *builder.File, client *client.Config, diags *diag.Diagnostics, index int) loki.LogsReceiver {
	label := fmt.Sprintf("default_%d", index)
	lokiWriteArgs := toLokiWriteArguments(client, diags)
	f.Body().AppendBlock(common.NewBlockWithOverride([]string{"loki", "write"}, label, lokiWriteArgs))
	return common.ConvertLogsReceiver{
		Expr: fmt.Sprintf("loki.write.%s.receiver", label),
	}
}

func toLokiWriteArguments(config *client.Config, diags *diag.Diagnostics) *lokiwrite.Arguments {
	batchSize, err := units.ParseBase2Bytes(fmt.Sprintf("%dB", config.BatchSize))
	if err != nil {
		diags.Add(
			diag.SeverityLevelError,
			fmt.Sprintf("failed to parse BatchSize for client config %s: %s", config.Name, err.Error()),
		)
	}

	// This is not supported yet - see https://github.com/grafana/agent/issues/4335.
	if config.DropRateLimitedBatches {
		diags.Add(
			diag.SeverityLevelError,
			"DropRateLimitedBatches is currently not supported in Grafana Agent Flow.",
		)
	}

	// Also deprecated in promtail.
	if len(config.StreamLagLabels) != 0 {
		diags.Add(
			diag.SeverityLevelWarn,
			"stream_lag_labels is deprecated and the associated metric has been removed",
		)
	}

	return &lokiwrite.Arguments{
		Endpoints: []lokiwrite.EndpointOptions{
			{
				Name:              config.Name,
				URL:               config.URL.String(),
				BatchWait:         config.BatchWait,
				BatchSize:         batchSize,
				HTTPClientConfig:  prometheusconvert.ToHttpClientConfig(&config.Client),
				Headers:           config.Headers,
				MinBackoff:        config.BackoffConfig.MinBackoff,
				MaxBackoff:        config.BackoffConfig.MaxBackoff,
				MaxBackoffRetries: config.BackoffConfig.MaxRetries,
				RemoteTimeout:     config.Timeout,
				TenantID:          config.TenantID,
			},
		},
		ExternalLabels: convertFlagLabels(config.ExternalLabels),
	}
}

func convertFlagLabels(labels lokiflag.LabelSet) map[string]string {
	result := map[string]string{}
	for k, v := range labels.LabelSet {
		result[string(k)] = string(v)
	}
	return result
}
