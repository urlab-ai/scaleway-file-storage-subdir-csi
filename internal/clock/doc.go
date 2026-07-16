// Package clock defines cancellable time boundaries used by polling and
// coordination. The manual implementation advances only when a test requests
// it, so retry and deadline tests never depend on fixed sleeps.
package clock
