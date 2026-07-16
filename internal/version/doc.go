// Package version validates build identity shared by the driver and csi-admin.
// Runtime VERSION is strict unprefixed SemVer; the optional v belongs only to a
// separately validated human release tag. Release builds inject the complete
// commit and canonical UTC time through linker flags. Development builds
// deliberately report their uncommitted identity instead of pretending to be
// a published artifact.
package version
