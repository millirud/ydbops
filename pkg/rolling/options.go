package rolling

import (
	"fmt"
	"math"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydbops/pkg/options"
	"github.com/ydb-platform/ydbops/pkg/utils"
)

const (
	DefaultDrainWaitTimeout        = 15 * time.Minute
	DrainWaitTimeoutActionStop     = "stop"
	DrainWaitTimeoutActionForce    = "force"
	DefaultRetryCount              = 3
	DefaultCMSQueryIntervalSeconds = 10
	DefaultRestartDurationSeconds  = 60
	DefaultNodesInflight           = 1
	DefaultDelayBetweenRestarts    = time.Second
	DefaultTenantsInflight         = 0
)

type RestartOptions struct {
	WithDrain              bool
	DrainWaitTimeout       time.Duration
	DrainWaitTimeoutAction string
	options.TargetingOptions

	RestartRetryNumber         int
	CMSQueryInterval           int
	NodesInflight              int
	DelayBetweenRestarts       time.Duration
	SuppressCompatibilityCheck bool
	CleanupOnExit              bool

	TenantsInflight int

	RestartDuration int

	SSHArgs []string

	CustomSystemdUnitName string
}

var rawSSHUnparsedArgs string

func (o *RestartOptions) Validate() error {
	if o.DrainWaitTimeout <= 0 {
		return fmt.Errorf("--drain-wait-timeout must be positive")
	}
	if o.DrainWaitTimeoutAction != DrainWaitTimeoutActionStop && o.DrainWaitTimeoutAction != DrainWaitTimeoutActionForce {
		return fmt.Errorf("invalid --drain-wait-timeout-action %q: available choices: stop, force", o.DrainWaitTimeoutAction)
	}

	err := o.TargetingOptions.Validate()
	if err != nil {
		return err
	}

	if o.CMSQueryInterval < 0 {
		return fmt.Errorf("specified invalid cms query interval seconds: %d. Must be positive", o.CMSQueryInterval)
	}

	if o.RestartRetryNumber < 0 {
		return fmt.Errorf("specified invalid restart retry number: %d. Must be positive", o.RestartRetryNumber)
	}

	if o.RestartDuration < 0 {
		return fmt.Errorf("specified invalid restart duration: %d. Must be positive", o.RestartDuration)
	}

	if o.TenantsInflight < 0 {
		return fmt.Errorf("specified invalid inflight tenants: %d. Must be positive", o.TenantsInflight)
	}

	o.SSHArgs = utils.ParseSSHArgs(rawSSHUnparsedArgs)

	return nil
}

func (o *RestartOptions) DefineFlags(fs *pflag.FlagSet) {
	o.TargetingOptions.DefineFlags(fs)
	fs.BoolVar(&o.WithDrain, "with-drain", false, "Before restarting a node, create a separate CMS drain task so tablets move away from it.")
	fs.DurationVar(&o.DrainWaitTimeout, "drain-wait-timeout", DefaultDrainWaitTimeout, "How long to wait for a node drain to finish before applying --drain-wait-timeout-action. Examples: 5s, 10m, 3h.")
	fs.StringVar(&o.DrainWaitTimeoutAction, "drain-wait-timeout-action", DrainWaitTimeoutActionStop, "What to do when --drain-wait-timeout is exceeded: stop returns an error; force logs a warning and continues the restart.")

	fs.StringVar(&o.CustomSystemdUnitName, "systemd-unit", "", "Specify custom systemd unit name to restart")

	fs.StringVar(&rawSSHUnparsedArgs, "ssh-args", "",
		`This argument will be used when ssh-ing to the nodes. It may be used to override
the ssh command itself, ssh username or any additional arguments.
Double quotes are can be escaped with backward slash '\'.
Examples:
1) --ssh-args "pssh -A -J <some jump host> --yc-profile <YC profile name>"
2) --ssh-args "ssh -o ProxyCommand=\"...\""`)

	fs.IntVar(&o.RestartRetryNumber, "restart-retry-number", DefaultRetryCount,
		fmt.Sprintf("How many times a node should be retried on error, default %v", DefaultRetryCount))

	fs.IntVar(&o.CMSQueryInterval, "cms-query-interval", DefaultCMSQueryIntervalSeconds,
		fmt.Sprintf("How often to query CMS while waiting for new permissions %v", DefaultCMSQueryIntervalSeconds))

	fs.IntVar(&o.RestartDuration, "duration", DefaultRestartDurationSeconds,
		`CMS will release the node for maintenance for duration * restart-retry-number seconds. Any maintenance
after that would be considered a regular cluster failure`)

	fs.BoolVar(&o.SuppressCompatibilityCheck, "suppress-compat-check", false,
		`By default, nodes within one cluster can differ by at most one major release.
ydbops will try to figure out if you broke this rule by comparing before\after of some restarted node.`)

	fs.IntVar(&o.NodesInflight, "nodes-inflight", DefaultNodesInflight,
		`The limit on the number of simultaneous node restarts. When --tenants-inflight is set, this is a per-tenant limit: each tenant gets up to this many tenant nodes restarted in parallel.`)

	fs.DurationVar(&o.DelayBetweenRestarts, "delay-between-restarts", DefaultDelayBetweenRestarts,
		`Delay between two consecutive restarts. E.g. '60s', '2m'. The number of simultaneous restarts is limited by 'nodes-inflight'.`)

	fs.BoolVar(&o.CleanupOnExit, "cleanup-on-exit", true,
		`When enabled, attempt to drop the maintenance task if the utility is killed by SIGTERM.`)

	fs.IntVar(&o.TenantsInflight, "tenants-inflight", DefaultTenantsInflight,
		`The number of tenants (databases) to restart concurrently. 
Each tenant gets up to --nodes-inflight parallel restarts. 
Default 0 means no grouping by tenant, restarting with global --nodes-inflight`)
}

func (o *RestartOptions) GetRestartDuration(nNodes int) *durationpb.Duration {
	singleBatchRestartTime := time.Second * time.Duration(o.RestartDuration) * time.Duration(o.RestartRetryNumber)
	if o.WithDrain {
		singleBatchRestartTime += o.DrainWaitTimeout * time.Duration(o.RestartRetryNumber)
	}
	singleBatchWithWait := singleBatchRestartTime + o.DelayBetweenRestarts
	maximumTotalBatches := int(math.Ceil(float64(nNodes) / float64(o.NodesInflight)))

	finalDuration := time.Duration(maximumTotalBatches) * singleBatchWithWait
	return durationpb.New(finalDuration)
}
