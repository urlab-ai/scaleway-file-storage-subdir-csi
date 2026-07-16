// Command csi-admin performs version-compatible operator workflows.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/admincli"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		if err := version.ValidateBuildIdentity(); err != nil {
			fmt.Fprintf(os.Stderr, "csi-admin: invalid build identity: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(version.String())
		return
	}
	if len(os.Args) == 2 && (os.Args[1] == "help" || os.Args[1] == "--help" || os.Args[1] == "-h") {
		fmt.Print(admincli.Usage())
		return
	}
	if err := version.ValidateBuildIdentity(); err != nil {
		fmt.Fprintf(os.Stderr, "csi-admin: invalid build identity: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := admincli.RunWithIO(ctx, os.Args[1:], os.Stdin, os.Stdout, version.Version); err != nil {
		fmt.Fprintf(os.Stderr, "csi-admin: %v\n", err)
		exitCode := admincli.ExitCode(err)
		if exitCode == 2 {
			fmt.Fprint(os.Stderr, admincli.Usage())
		}
		os.Exit(exitCode)
	}
}
