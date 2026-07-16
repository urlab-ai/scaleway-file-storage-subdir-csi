// Command csi-kind-fake runs the development-only CSI endpoint used by the
// disposable kind integration suite. It is never included in release images.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"scaleway-sfs-subdir-csi/internal/kindfake"
	"scaleway-sfs-subdir-csi/pkg/config"
)

func main() {
	flags := flag.NewFlagSet("csi-kind-fake", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	mode := flags.String("mode", "", "controller or node")
	endpoint := flags.String("endpoint", "", "unix:///absolute/csi.sock")
	driverName := flags.String("driver-name", "", "test CSI driver name")
	nodeName := flags.String("node-name", "", "Kubernetes node name")
	dataRoot := flags.String("data-root", "", "host-backed fake data root")
	kubeletPath := flags.String("kubelet-path", "", "kubelet root")
	liveAddress := flags.String("live-address", "", "numeric liveness listener")
	if err := flags.Parse(os.Args[1:]); err != nil || flags.NArg() != 0 {
		if err == nil {
			err = fmt.Errorf("positional arguments are unsupported")
		}
		fmt.Fprintf(os.Stderr, "csi-kind-fake: %v\n", err)
		os.Exit(2)
	}
	const unixPrefix = "unix://"
	if !strings.HasPrefix(*endpoint, unixPrefix) {
		fmt.Fprintln(os.Stderr, "csi-kind-fake: --endpoint must use unix:///absolute/path")
		os.Exit(2)
	}
	options := kindfake.Options{
		Component: config.Component(*mode), CSIEndpointPath: strings.TrimPrefix(*endpoint, unixPrefix),
		DriverName: *driverName, NodeName: *nodeName, DataRoot: *dataRoot,
		KubeletPath: *kubeletPath, LiveAddress: *liveAddress,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := kindfake.Run(ctx, options); err != nil {
		fmt.Fprintf(os.Stderr, "csi-kind-fake: %v\n", err)
		os.Exit(1)
	}
}
