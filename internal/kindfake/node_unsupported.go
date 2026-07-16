//go:build !linux

package kindfake

import "fmt"

func newNodeCore(Options) (nodeServiceCore, error) {
	return nil, fmt.Errorf("kind fake node endpoint requires Linux")
}
