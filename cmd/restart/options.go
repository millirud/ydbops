package restart

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydbops/pkg/cmdutil"
	"github.com/ydb-platform/ydbops/pkg/options"
	"github.com/ydb-platform/ydbops/pkg/rolling"
	"github.com/ydb-platform/ydbops/pkg/rolling/restarters"
)

type Options struct {
	*rolling.RestartOptions
}

func (o *Options) DefineFlags(fs *pflag.FlagSet) {
	o.RestartOptions.DefineFlags(fs)

	fs.BoolVar(&o.WithDrain, "with-drain", false,
		`Before restarting a node, create a separate CMS drain task so tablets move away from it.`)

	fs.DurationVar(&o.DrainWaitTimeout, "drain-wait-timeout", rolling.DefaultDrainWaitTimeout,
		`How long to wait for a node drain to finish before applying --drain-wait-timeout-action. E.g. '5s', '10m', '3h'.`)

	fs.StringVar(&o.DrainWaitTimeoutAction, "drain-wait-timeout-action", rolling.DrainWaitTimeoutActionSkip,
		fmt.Sprintf(`What to do when --drain-wait-timeout is exceeded. Available choices: %s, %s.
%s stops the restart of the node and returns an error.
%s logs a warning and continues the restart.`,
			rolling.DrainWaitTimeoutActionFail, rolling.DrainWaitTimeoutActionSkip,
			rolling.DrainWaitTimeoutActionFail, rolling.DrainWaitTimeoutActionSkip))
}

func PrepareRestarters(
	opts *options.TargetingOptions,
	sshArgs []string,
	customSystemdUnitName string,
	restartDuration int,
) (storage, tenant restarters.Restarter) {
	if opts.KubeconfigPath != "" {
		storage = restarters.NewStorageK8sRestarter(
			options.Logger,
			&restarters.StorageK8sRestarterOptions{
				K8sRestarterOptions: &restarters.K8sRestarterOptions{
					KubeconfigPath:  opts.KubeconfigPath,
					Namespace:       opts.K8sNamespace,
					RestartDuration: time.Duration(restartDuration) * time.Second,
				},
			},
		)
		tenant = restarters.NewTenantK8sRestarter(
			options.Logger,
			&restarters.TenantK8sRestarterOptions{
				K8sRestarterOptions: &restarters.K8sRestarterOptions{
					KubeconfigPath:  opts.KubeconfigPath,
					Namespace:       opts.K8sNamespace,
					RestartDuration: time.Duration(restartDuration) * time.Second,
				},
			},
		)
		return storage, tenant
	}

	storage = restarters.NewStorageSSHRestarter(
		options.Logger,
		sshArgs,
		customSystemdUnitName,
	)
	tenant = restarters.NewTenantSSHRestarter(
		options.Logger,
		sshArgs,
		customSystemdUnitName,
	)
	return storage, tenant
}

func (o *Options) Run(f cmdutil.Factory) error {
	storageRestarter, tenantRestarter := PrepareRestarters(
		&o.TargetingOptions,
		o.SSHArgs,
		o.CustomSystemdUnitName,
		o.RestartDuration,
	)

	bothUnspecified := !o.Storage && !o.Tenant

	var executer rolling.Executer
	var err error
	if o.Storage || bothUnspecified {
		// TODO(shmel1k@): add logger to NewExecuter parameters
		executer = rolling.NewExecuter(o.RestartOptions, zap.S(), f.GetCMSClient(), f.GetDiscoveryClient(), storageRestarter)
		err = executer.Execute()
	}

	if err != nil {
		return err
	}

	if o.Tenant || bothUnspecified {
		executer = rolling.NewExecuter(o.RestartOptions, zap.S(), f.GetCMSClient(), f.GetDiscoveryClient(), tenantRestarter)
		err = executer.Execute()
	}

	return err
}
