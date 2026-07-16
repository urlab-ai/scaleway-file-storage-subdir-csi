// Package parentfs adapts one controller-mounted parent filesystem to the
// driver's durable ownership and logical-directory state machines. Provider
// attachment and mount authorization stay outside this package; every local
// metadata or data operation is re-anchored below the exact returned mount root
// with the no-follow safety backends.
package parentfs
