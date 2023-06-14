package build

import (
	"github.com/grafana/agent/component/common/loki"
	"time"
)

type GlobalContext struct {
	WriteReceivers   []loki.LogsReceiver
	TargetSyncPeriod time.Duration
}
