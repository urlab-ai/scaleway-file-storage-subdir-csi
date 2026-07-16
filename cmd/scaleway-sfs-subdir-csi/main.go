// Command scaleway-sfs-subdir-csi runs the controller or node CSI endpoint.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"scaleway-sfs-subdir-csi/internal/driverapp"
	"scaleway-sfs-subdir-csi/internal/version"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		if err := version.ValidateBuildIdentity(); err != nil {
			fmt.Fprintf(os.Stderr, "scaleway-sfs-subdir-csi: invalid build identity: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(version.String())
		return
	}
	if len(os.Args) == 2 && (os.Args[1] == "help" || os.Args[1] == "--help" || os.Args[1] == "-h") {
		fmt.Print(driverapp.Usage())
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, err := driverapp.Load(ctx, os.Args[1:], os.LookupEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scaleway-sfs-subdir-csi: %v\n", err)
		exitCode := driverapp.ExitCode(err)
		if exitCode == 2 {
			fmt.Fprint(os.Stderr, driverapp.Usage())
		}
		os.Exit(exitCode)
	}

	if err := driverapp.Run(ctx, startup); err != nil {
		fmt.Fprintf(os.Stderr, "scaleway-sfs-subdir-csi: run %s component: %v\n", startup.Options.Component, err)
		os.Exit(1)
	}
}
