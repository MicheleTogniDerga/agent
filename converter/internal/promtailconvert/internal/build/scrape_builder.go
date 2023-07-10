package build

import (
	"fmt"
	"strings"
	"time"

	"github.com/grafana/agent/component/common/loki"
	flowrelabel "github.com/grafana/agent/component/common/relabel"
	"github.com/grafana/agent/component/discovery"
	discoveryfile "github.com/grafana/agent/component/discovery/file"
	"github.com/grafana/agent/component/discovery/relabel"
	"github.com/grafana/agent/component/loki/process"
	"github.com/grafana/agent/component/loki/process/stages"
	lokirelabel "github.com/grafana/agent/component/loki/relabel"
	"github.com/grafana/agent/component/loki/source/cloudflare"
	lokisourcefile "github.com/grafana/agent/component/loki/source/file"
	"github.com/grafana/agent/component/loki/source/journal"
	"github.com/grafana/agent/converter/diag"
	"github.com/grafana/agent/converter/internal/common"
	"github.com/grafana/agent/converter/internal/prometheusconvert"
	"github.com/grafana/agent/pkg/river/rivertypes"
	"github.com/grafana/agent/pkg/river/token/builder"
	"github.com/grafana/loki/clients/pkg/promtail/scrapeconfig"
	"github.com/prometheus/common/model"
)

type ScrapeConfigBuilder struct {
	f         *builder.File
	diags     *diag.Diagnostics
	cfg       *scrapeconfig.Config
	globalCtx *GlobalContext

	allTargetsExps             []string
	processStageReceivers      []loki.LogsReceiver
	allRelabeledTargetsExpr    string
	allExpandedFileTargetsExpr string
	discoveryRelabelRulesExpr  string
	lokiRelabelReceiverExpr    string
}

func NewScrapeConfigBuilder(
	f *builder.File,
	diags *diag.Diagnostics,
	cfg *scrapeconfig.Config,
	globalCtx *GlobalContext,

) *ScrapeConfigBuilder {

	return &ScrapeConfigBuilder{
		f:         f,
		diags:     diags,
		cfg:       cfg,
		globalCtx: globalCtx,
	}
}

func (s *ScrapeConfigBuilder) AppendCloudFlareConfig() {
	if s.cfg.CloudflareConfig == nil {
		return
	}

	args := cloudflare.Arguments{
		APIToken:   rivertypes.Secret(s.cfg.CloudflareConfig.APIToken),
		ZoneID:     s.cfg.CloudflareConfig.ZoneID,
		Labels:     convertPromLabels(s.cfg.CloudflareConfig.Labels),
		Workers:    s.cfg.CloudflareConfig.Workers,
		PullRange:  time.Duration(s.cfg.CloudflareConfig.PullRange),
		FieldsType: s.cfg.CloudflareConfig.FieldsType,
	}
	override := func(val interface{}) interface{} {
		switch conv := val.(type) {
		case []loki.LogsReceiver:
			return common.CustomTokenizer{Expr: fmt.Sprintf("[%s]", s.getOrNewLokiRelabel())}
		case rivertypes.Secret:
			return string(conv)
		default:
			return val
		}
	}
	s.f.Body().AppendBlock(common.NewBlockWithOverrideFn(
		[]string{"loki", "source", "cloudfare"},
		s.cfg.JobName,
		args,
		override,
	))
}

func (s *ScrapeConfigBuilder) AppendJournalConfig() {
	jc := s.cfg.JournalConfig
	if jc == nil {
		return
	}
	maxAge, err := time.ParseDuration(jc.MaxAge)
	if err != nil {
		s.diags.Add(
			diag.SeverityLevelError,
			fmt.Sprintf("failed to parse max_age duration for journal config: %s, will use default", err),
		)
		maxAge = time.Hour * 7 // use default value
	}
	args := journal.Arguments{
		FormatAsJson: jc.JSON,
		MaxAge:       maxAge,
		Path:         jc.Path,
		Receivers:    s.getOrNewProcessStageReceivers(),
		Labels:       convertPromLabels(jc.Labels),
		RelabelRules: flowrelabel.Rules{},
	}
	relabelRulesExpr := s.getOrNewDiscoveryRelabelRules()
	hook := func(val interface{}) interface{} {
		if _, ok := val.(flowrelabel.Rules); ok {
			return common.CustomTokenizer{Expr: relabelRulesExpr}
		}
		return val
	}
	s.f.Body().AppendBlock(common.NewBlockWithOverrideFn(
		[]string{"loki", "source", "journal"},
		s.cfg.JobName,
		args,
		hook,
	))
}

func (s *ScrapeConfigBuilder) AppendKubernetesSDs() {
	if len(s.cfg.ServiceDiscoveryConfig.KubernetesSDConfigs) == 0 {
		return
	}

	for i, sd := range s.cfg.ServiceDiscoveryConfig.KubernetesSDConfigs {
		s.diags.AddAll(prometheusconvert.ValidateHttpClientConfig(&sd.HTTPClientConfig))
		args := prometheusconvert.ToDiscoveryKubernetes(sd)
		compName := fmt.Sprintf("%s_%d", s.cfg.JobName, i)
		s.f.Body().AppendBlock(common.NewBlockWithOverride(
			[]string{"discovery", "kubernetes"},
			compName,
			args,
		))
		s.allTargetsExps = append(s.allTargetsExps, "discovery.kubernetes."+compName+".targets")
	}
}

func (s *ScrapeConfigBuilder) AppendLokiSourceFile() {
	// If there were no targets expressions collected, that means
	// we didn't have any components that produced SD targets, so
	// we can skip this component.
	if len(s.allTargetsExps) == 0 {
		return
	}
	targets := s.getExpandedFileTargetsExpr()
	forwardTo := s.getOrNewProcessStageReceivers()

	args := lokisourcefile.Arguments{
		ForwardTo: forwardTo,
	}
	overrideHook := func(val interface{}) interface{} {
		if _, ok := val.([]discovery.Target); ok {
			return common.CustomTokenizer{Expr: targets}
		}
		return val
	}

	s.f.Body().AppendBlock(common.NewBlockWithOverrideFn(
		[]string{"loki", "source", "file"},
		s.cfg.JobName,
		args,
		overrideHook,
	))
}

func (s *ScrapeConfigBuilder) getOrNewLokiRelabel() string {
	if len(s.cfg.RelabelConfigs) == 0 {
		// If no relabels - we can send straight to the process stage.
		return logsReceiversToExpr(s.getOrNewProcessStageReceivers())
	}

	if s.lokiRelabelReceiverExpr == "" {
		args := lokirelabel.Arguments{
			ForwardTo:      s.getOrNewProcessStageReceivers(),
			RelabelConfigs: prometheusconvert.ToFlowRelabelConfigs(s.cfg.RelabelConfigs),
		}
		s.f.Body().AppendBlock(common.NewBlockWithOverride([]string{"loki", "relabel"}, s.cfg.JobName, args))
		s.lokiRelabelReceiverExpr = "loki.relabel." + s.cfg.JobName + ".receiver"
	}
	return s.lokiRelabelReceiverExpr
}

func (s *ScrapeConfigBuilder) getOrNewProcessStageReceivers() []loki.LogsReceiver {
	if s.processStageReceivers != nil {
		return s.processStageReceivers
	}
	if len(s.cfg.PipelineStages) == 0 {
		s.processStageReceivers = s.globalCtx.WriteReceivers
		return s.processStageReceivers
	}

	flowStages := make([]stages.StageConfig, len(s.cfg.PipelineStages))
	for i, ps := range s.cfg.PipelineStages {
		if fs, ok := convertStage(ps, s.diags); ok {
			flowStages[i] = fs
		}
	}
	args := process.Arguments{
		ForwardTo: s.globalCtx.WriteReceivers,
		Stages:    flowStages,
	}
	s.f.Body().AppendBlock(common.NewBlockWithOverride([]string{"loki", "process"}, s.cfg.JobName, args))
	s.processStageReceivers = []loki.LogsReceiver{common.ConvertLogsReceiver{
		Expr: fmt.Sprintf("loki.process.%s.receiver", s.cfg.JobName),
	}}
	return s.processStageReceivers
}

func (s *ScrapeConfigBuilder) appendDiscoveryRelabel() {
	if s.allRelabeledTargetsExpr != "" {
		return
	}
	if len(s.cfg.RelabelConfigs) == 0 {
		// Skip the discovery.relabel component if there are no relabels needed
		s.allRelabeledTargetsExpr, s.discoveryRelabelRulesExpr = s.getAllTargetsJoinedExpr(), "null"
		return
	}

	relabelConfigs := prometheusconvert.ToFlowRelabelConfigs(s.cfg.RelabelConfigs)
	args := relabel.Arguments{
		RelabelConfigs: relabelConfigs,
	}

	overrideHook := func(val interface{}) interface{} {
		if _, ok := val.([]discovery.Target); ok {
			return common.CustomTokenizer{Expr: s.getAllTargetsJoinedExpr()}
		}
		return val
	}

	s.f.Body().AppendBlock(common.NewBlockWithOverrideFn(
		[]string{"discovery", "relabel"},
		s.cfg.JobName,
		args,
		overrideHook,
	))
	compName := fmt.Sprintf("discovery.relabel.%s", s.cfg.JobName)
	s.allRelabeledTargetsExpr, s.discoveryRelabelRulesExpr = compName+".output", compName+".rules"
}

func (s *ScrapeConfigBuilder) getAllRelabeledTargetsExpr() string {
	s.appendDiscoveryRelabel()
	return s.allRelabeledTargetsExpr
}

func (s *ScrapeConfigBuilder) getOrNewDiscoveryRelabelRules() string {
	s.appendDiscoveryRelabel()
	return s.discoveryRelabelRulesExpr
}

func (s *ScrapeConfigBuilder) getExpandedFileTargetsExpr() string {
	if s.allExpandedFileTargetsExpr != "" {
		return s.allExpandedFileTargetsExpr
	}
	args := discoveryfile.Arguments{
		SyncPeriod: s.globalCtx.TargetSyncPeriod,
	}
	overrideHook := func(val interface{}) interface{} {
		if _, ok := val.([]discovery.Target); ok {
			return common.CustomTokenizer{Expr: s.getAllRelabeledTargetsExpr()}
		}
		return val
	}

	s.f.Body().AppendBlock(common.NewBlockWithOverrideFn(
		[]string{"discovery", "file"},
		s.cfg.JobName,
		args,
		overrideHook,
	))
	s.allExpandedFileTargetsExpr = "discovery.file." + s.cfg.JobName + ".targets"
	return s.allExpandedFileTargetsExpr
}

func (s *ScrapeConfigBuilder) getAllTargetsJoinedExpr() string {
	targetsExpr := "[]"
	if len(s.allTargetsExps) == 1 {
		targetsExpr = s.allTargetsExps[0]
	} else if len(s.allTargetsExps) > 1 {
		targetsExpr = fmt.Sprintf("concat(%s)", strings.Join(s.allTargetsExps, ", "))
	}
	return targetsExpr
}

func convertPromLabels(labels model.LabelSet) map[string]string {
	result := make(map[string]string)
	for k, v := range labels {
		result[string(k)] = string(v)
	}
	return result
}

func logsReceiversToExpr(r []loki.LogsReceiver) string {
	var exprs []string
	for _, r := range r {
		clr := r.(*common.ConvertLogsReceiver)
		exprs = append(exprs, clr.Expr)
	}
	return "[" + strings.Join(exprs, ", ") + "]"
}
