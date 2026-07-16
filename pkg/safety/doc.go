// Package safety owns path confinement, crash-durable metadata installation,
// lifecycle durability barriers, and destructive traversal safeguards.
//
// Callers pass only validated volume identity. This package independently
// rejects absolute or traversing relative paths and never offers a generic
// unscoped remove or rename API to CSI services.
package safety
