// Command scaleway-e2e-cleanup validates retained E2E inventory and prints an
// exact-ID, non-authorizing cleanup review. It contains no mutation backend.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const maxInventoryBytes = 128 * 1024

func main() {
	if err := run(os.Args[1:], os.Stdout, time.Now()); err != nil {
		fmt.Fprintf(os.Stderr, "scaleway-e2e-cleanup: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer, now time.Time) error {
	if output == nil {
		return fmt.Errorf("E2E cleanup output is nil")
	}
	inventoryPath, err := arguments(args)
	if err != nil {
		return err
	}
	data, err := readBoundedRegularFile(inventoryPath)
	if err != nil {
		return err
	}
	var inventory e2ecleanup.Inventory
	if err := strictjson.Decode(data, &inventory); err != nil {
		return fmt.Errorf("decode E2E cleanup inventory: %w", err)
	}
	plan, err := e2ecleanup.Build(inventory, now)
	if err != nil {
		return fmt.Errorf("validate E2E cleanup inventory: %w", err)
	}
	encoded, err := e2ecleanup.Encode(plan)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return writeOutput(output, encoded, "E2E cleanup review")
}

func writeOutput(output io.Writer, data []byte, description string) error {
	for len(data) > 0 {
		written, err := output.Write(data)
		if err != nil {
			return fmt.Errorf("write %s: %w", description, err)
		}
		if written <= 0 || written > len(data) {
			return fmt.Errorf("write %s: invalid write count %d", description, written)
		}
		data = data[written:]
	}
	return nil
}

func arguments(args []string) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("usage: scaleway-e2e-cleanup --inventory=/absolute/inventory.json --dry-run")
	}
	var inventoryPath string
	dryRun := false
	for _, argument := range args {
		switch {
		case argument == "--dry-run" && !dryRun:
			dryRun = true
		case strings.HasPrefix(argument, "--inventory=") && inventoryPath == "":
			inventoryPath = strings.TrimPrefix(argument, "--inventory=")
		default:
			return "", fmt.Errorf("usage: scaleway-e2e-cleanup --inventory=/absolute/inventory.json --dry-run")
		}
	}
	if !dryRun || inventoryPath == "" {
		return "", fmt.Errorf("usage: scaleway-e2e-cleanup --inventory=/absolute/inventory.json --dry-run")
	}
	if err := e2ecleanup.ValidateInventoryPath(inventoryPath); err != nil {
		return "", err
	}
	return inventoryPath, nil
}

func readBoundedRegularFile(filename string) (data []byte, returnErr error) {
	before, err := os.Lstat(filename)
	if err != nil {
		return nil, fmt.Errorf("inspect inventory: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("inventory must be an exact regular file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open inventory: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	after, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened inventory: %w", err)
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, fmt.Errorf("inventory changed during open")
	}
	if after.Size() <= 0 || after.Size() > maxInventoryBytes {
		return nil, fmt.Errorf("inventory must contain 1 to %d bytes", maxInventoryBytes)
	}
	data, err = io.ReadAll(io.LimitReader(file, maxInventoryBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read inventory: %w", err)
	}
	final, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("restat inventory after read: %w", err)
	}
	if !os.SameFile(after, final) || after.Size() != final.Size() || !after.ModTime().Equal(final.ModTime()) || final.Size() != int64(len(data)) {
		return nil, fmt.Errorf("inventory changed during read")
	}
	if len(data) == 0 || len(data) > maxInventoryBytes {
		return nil, fmt.Errorf("inventory must contain 1 to %d bytes", maxInventoryBytes)
	}
	return data, nil
}
