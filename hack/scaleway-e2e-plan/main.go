// Command scaleway-e2e-plan validates and prints non-authorizing real-cloud
// E2E review evidence. It contains no execution backend by design.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const maxInputBytes = 64 * 1024

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "scaleway-e2e-plan: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if output == nil {
		return fmt.Errorf("E2E plan output is nil")
	}
	input, err := inputPath(args)
	if err != nil {
		return err
	}
	data, err := readBoundedRegularFile(input)
	if err != nil {
		return err
	}
	var request e2eplan.Request
	if err := strictjson.Decode(data, &request); err != nil {
		return fmt.Errorf("decode E2E plan request: %w", err)
	}
	plan, err := e2eplan.Build(request)
	if err != nil {
		return fmt.Errorf("validate E2E plan request: %w", err)
	}
	encoded, err := e2eplan.Encode(plan)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return writeOutput(output, encoded, "E2E plan")
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

func inputPath(args []string) (string, error) {
	if len(args) != 1 || !strings.HasPrefix(args[0], "--input=") {
		return "", fmt.Errorf("usage: scaleway-e2e-plan --input=/absolute/request.json")
	}
	value := strings.TrimPrefix(args[0], "--input=")
	if value == "" || value == string(filepath.Separator) || !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("input path must be clean, absolute, and non-root")
	}
	return value, nil
}

func readBoundedRegularFile(filename string) (data []byte, returnErr error) {
	before, err := os.Lstat(filename)
	if err != nil {
		return nil, fmt.Errorf("inspect input: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("input must be an exact regular file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open input: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	after, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened input: %w", err)
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, fmt.Errorf("input changed during open")
	}
	if after.Size() <= 0 || after.Size() > maxInputBytes {
		return nil, fmt.Errorf("input must contain 1 to %d bytes", maxInputBytes)
	}
	data, err = io.ReadAll(io.LimitReader(file, maxInputBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read input: %w", err)
	}
	final, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("restat input after read: %w", err)
	}
	if !os.SameFile(after, final) || after.Size() != final.Size() || !after.ModTime().Equal(final.ModTime()) || final.Size() != int64(len(data)) {
		return nil, fmt.Errorf("input changed during read")
	}
	if len(data) == 0 || len(data) > maxInputBytes {
		return nil, fmt.Errorf("input must contain 1 to %d bytes", maxInputBytes)
	}
	return data, nil
}
