// Package scaleway implements the authenticated provider boundary for existing
// File Storage parents and Instance attachments, plus the distinct
// credential-free local metadata projection used by the node plugin.
//
// Provider observations are normalized into closed internal enums before they
// influence a mutation. Unknown SDK values remain unknown and fail closed until
// a compatibility-tested release explicitly maps them. Node runtime wiring may
// receive only MetadataSource and NodeIdentity; it must never receive the
// authenticated API implementation or Scaleway credentials.
package scaleway
