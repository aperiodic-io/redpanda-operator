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
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/spf13/afero"
)

func TestMergeConfiguredSuperusers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		bootstrap string
		missing   bool
		users     []string
		want      []string
		wantErr   bool
	}{
		{
			name:      "anonymous and named configured users survive secret resync",
			bootstrap: "superusers: [\"\", configured, svc-admin]\nkafka_enable_authorization: true\n",
			users:     []string{"svc-admin", "svc-ops"},
			want:      []string{"svc-admin", "svc-ops", "", "configured"},
		},
		{
			name:      "duplicate declarations do not duplicate privileges",
			bootstrap: "superusers: [\"\", \"\", svc-admin]\n",
			users:     []string{"svc-admin"}, want: []string{"svc-admin", ""},
		},
		{
			name: "missing bootstrap preserves legacy behavior", missing: true,
			users: []string{"svc-admin"}, want: []string{"svc-admin"},
		},
		{
			name:      "no declared users preserves secret users",
			bootstrap: "kafka_enable_authorization: true\n",
			users:     []string{"svc-admin"}, want: []string{"svc-admin"},
		},
		{
			name:      "invalid configuration cannot silently remove privileges",
			bootstrap: "superusers: [", users: []string{"svc-admin"}, wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			configPath := "/custom/redpanda.yaml"
			if err := fs.MkdirAll(path.Dir(configPath), 0o755); err != nil {
				t.Fatal(err)
			}
			if !tc.missing {
				if err := afero.WriteFile(fs, "/custom/.bootstrap.yaml", []byte(tc.bootstrap), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			got, err := mergeConfiguredSuperusers(fs, configPath, tc.users)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("superusers = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConfiguredSuperusersDoNotRetainRemovedUsers(t *testing.T) {
	fs := afero.NewMemMapFs()
	if err := afero.WriteFile(fs, "/.bootstrap.yaml", []byte("superusers: [\"\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := mergeConfiguredSuperusers(fs, "/redpanda.yaml", []string{"svc-admin", "removed"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, []string{"svc-admin", "removed", ""}) {
		t.Fatal(first)
	}
	second, err := mergeConfiguredSuperusers(fs, "/redpanda.yaml", []string{"svc-admin"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, []string{"svc-admin", ""}) {
		t.Fatalf("removed secret user kept superuser access: %q", second)
	}
}

type unreadableBootstrapFS struct{ afero.Fs }

func (fs unreadableBootstrapFS) Open(name string) (afero.File, error) {
	if name == "/.bootstrap.yaml" {
		return nil, os.ErrPermission
	}
	return fs.Fs.Open(name)
}

func TestConfiguredSuperusersReadError(t *testing.T) {
	fs := unreadableBootstrapFS{afero.NewMemMapFs()}
	if _, err := mergeConfiguredSuperusers(fs, "/redpanda.yaml", []string{"svc-admin"}); err == nil {
		t.Fatal("unreadable bootstrap must not silently discard configured privileges")
	}
}
