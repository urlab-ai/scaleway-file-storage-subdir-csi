// Package compatibility owns the closed release-qualified provider compatibility
// declarations shared by build identity, runtime configuration, and Helm.
//
// These declarations are release evidence rather than operator acknowledgements:
// production startup requires the chart/runtime list to equal the list embedded
// in the exact driver binary.
package compatibility
