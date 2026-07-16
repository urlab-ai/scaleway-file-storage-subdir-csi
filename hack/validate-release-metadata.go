// Command validate-release-metadata validates and emits the canonical identity
// committed beside every release binary before cross-compilation begins.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	releasecompat "scaleway-sfs-subdir-csi/internal/compatibility"
	"scaleway-sfs-subdir-csi/internal/version"
)

func main() {
	metadata := version.ReleaseMetadata{}
	flag.StringVar(&metadata.ReleaseTag, "release-tag", "", "human Git release tag")
	flag.StringVar(&metadata.Version, "version", "", "unprefixed SemVer embedded in binaries")
	flag.StringVar(&metadata.Commit, "commit", "", "complete Git object ID")
	flag.StringVar(&metadata.BuildDate, "build-date", "", "canonical UTC build timestamp")
	commercialTypes := flag.String("commercial-types", "", "canonical comma-separated release-qualified commercial types")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "release metadata does not accept positional arguments")
		os.Exit(2)
	}
	parsedTypes, err := releasecompat.ParseCommercialTypes(*commercialTypes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid release commercial types: %v\n", err)
		os.Exit(2)
	}
	metadata.CommercialTypes = parsedTypes
	if err := metadata.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid release metadata: %v\n", err)
		os.Exit(2)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(metadata); err != nil {
		fmt.Fprintf(os.Stderr, "encode release metadata: %v\n", err)
		os.Exit(1)
	}
}
