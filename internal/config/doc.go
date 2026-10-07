// Package config defines Sylphy's runtime configuration: defaults, validation,
// and loading from command-line flags with environment-variable defaults.
//
// Precedence is flag > environment (SYLPHY_*) > built-in default.
package config
