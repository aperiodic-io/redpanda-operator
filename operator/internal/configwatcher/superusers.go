// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package configwatcher

import (
	"fmt"
	"os"
	"path"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/spf13/afero"
	"sigs.k8s.io/yaml"
)

// mergeConfiguredSuperusers preserves the chart's explicitly configured
// superusers, including the empty-name principal on unauthenticated listeners.
// Merge declarations, rather than the broker's current value, so removing a
// user from the Secret still removes its superuser privilege on the next sync.
func mergeConfiguredSuperusers(fs afero.Fs, configPath string, users []string) ([]string, error) {
	bootstrapPath := path.Join(path.Dir(configPath), ".bootstrap.yaml")
	data, err := afero.ReadFile(fs, bootstrapPath)
	if errors.Is(err, os.ErrNotExist) {
		// Older deployments and standalone watchers may not mount a bootstrap file.
		return users, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading configured superusers from %s: %w", bootstrapPath, err)
	}
	var bootstrap struct {
		Superusers []string `json:"superusers"`
	}
	if err := yaml.Unmarshal(data, &bootstrap); err != nil {
		return nil, fmt.Errorf("parsing configured superusers from %s: %w", bootstrapPath, err)
	}
	for _, user := range bootstrap.Superusers {
		if !slices.Contains(users, user) {
			users = append(users, user)
		}
	}
	return users, nil
}
