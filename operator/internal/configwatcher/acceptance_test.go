// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package configwatcher_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/redpanda-operator/operator/internal/configwatcher"
)

// TestConfiguredSuperusersAcceptance reproduces leader acquisition against an
// authorized plaintext Kafka listener, as used by idempotent metrics producers.
func TestConfiguredSuperusersAcceptance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const user, password, mechanism = "admin", "password", "SCRAM-SHA-512"
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v26.2.1",
		redpanda.WithSuperusers(user), redpanda.WithEnableKafkaAuthorization(), redpanda.WithAutoCreateTopics(),
		testcontainers.WithEnv(map[string]string{"RP_BOOTSTRAP_USER": fmt.Sprintf("%s:%s:%s", user, password, mechanism)}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
	adminAddress, err := container.AdminAPIAddress(ctx)
	require.NoError(t, err)
	admin, err := rpadmin.NewAdminAPI([]string{adminAddress}, &rpadmin.BasicAuth{Username: user, Password: password}, nil)
	require.NoError(t, err)
	defer admin.Close()
	broker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	// Negative control: anonymous idempotent-write is denied before the grant.
	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	request := kmsg.NewPtrInitProducerIDRequest()
	request.TransactionTimeoutMillis = 10000
	response, err := client.Request(ctx, request)
	require.NoError(t, err)
	require.Equal(t, kerr.ClusterAuthorizationFailed.Code, response.(*kmsg.InitProducerIDResponse).ErrorCode)
	t.Log("negative control: anonymous InitProducerID denied without declared grant")
	client.Close()

	t.Setenv("RPK_USER", user)
	t.Setenv("RPK_PASS", password)
	t.Setenv("RPK_SASL_MECHANISM", mechanism)
	configDir, usersDir := t.TempDir(), t.TempDir()
	configPath := filepath.Join(configDir, "redpanda.yaml")
	bootstrapPath := filepath.Join(configDir, ".bootstrap.yaml")
	usersPath := filepath.Join(usersDir, "users.txt")
	require.NoError(t, os.WriteFile(configPath, []byte(createRedpandaYaml(adminAddress, user, password, mechanism)), 0o600))
	require.NoError(t, os.WriteFile(bootstrapPath, []byte("superusers: [\"\", configured]\n"), 0o600))
	require.NoError(t, os.WriteFile(usersPath, []byte(createUserLine("removed", "password", mechanism)), 0o600))
	start := func() (*configwatcher.ConfigWatcher, func()) {
		watchCtx, stop := context.WithCancel(ctx)
		watcher := configwatcher.NewConfigWatcher(testr.New(t), false, configwatcher.WithRedpandaConfigPath(configPath), configwatcher.WithUsersDirectory(usersDir))
		done := make(chan error, 1)
		go func() { done <- watcher.Start(watchCtx) }()
		var once sync.Once
		shutdown := func() {
			once.Do(func() {
				stop()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("watcher did not stop")
				}
			})
		}
		t.Cleanup(shutdown)
		return watcher, shutdown
	}
	assertSuperusers := func(want []string) {
		t.Helper()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			config, err := admin.SingleKeyConfig(ctx, "superusers")
			require.NoError(c, err)
			require.ElementsMatch(c, want, config["superusers"])
		}, 10*time.Second, 100*time.Millisecond)
	}
	assertProduce := func(stage string) {
		t.Helper()
		// Default producers are idempotent; a fresh client must obtain a producer ID.
		producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation(), kgo.RecordDeliveryTimeout(10*time.Second))
		require.NoError(t, err)
		defer producer.Close()
		require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: "superusers-acceptance", Value: []byte(stage)}).FirstErr(), stage)
		t.Logf("anonymous idempotent produce succeeded: %s", stage)
	}
	watcher, stop := start()
	want := []string{user, "removed", "", "configured"}
	assertSuperusers(want)
	assertProduce("initial leader sync")
	watcher.SyncUsers(ctx, usersPath)
	assertSuperusers(want)
	assertProduce("repeated Secret sync")
	stop()
	// A new declaration makes startup reconciliation observable rather than
	// accepting the prior leader's unchanged config before Start completes.
	require.NoError(t, os.WriteFile(bootstrapPath, []byte("superusers: [\"\", configured, restarted]\n"), 0o600))
	want = append(want, "restarted")
	watcher, _ = start()
	assertSuperusers(want)
	assertProduce("fresh watcher after leadership reacquisition")
	require.NoError(t, os.WriteFile(usersPath, []byte(createUserLine("replacement", "password", mechanism)), 0o600))
	watcher.SyncUsers(ctx, usersPath)
	want = []string{user, "replacement", "", "configured", "restarted"}
	assertSuperusers(want)
	assertProduce("removed Secret user loses grant")
	// A failed bootstrap read must not patch a replacement list that drops
	// configured principals. Change the Secret to make an erroneous PATCH visible.
	require.NoError(t, os.WriteFile(usersPath, []byte(createUserLine("unexpected", "password", mechanism)), 0o600))
	require.NoError(t, os.WriteFile(bootstrapPath, []byte("superusers: ["), 0o600))
	watcher.SyncUsers(ctx, usersPath)
	assertSuperusers(want)
	assertProduce("malformed bootstrap leaves grants unchanged")
	require.NoError(t, os.Remove(bootstrapPath))
	require.NoError(t, os.Mkdir(bootstrapPath, 0o700))
	watcher.SyncUsers(ctx, usersPath)
	assertSuperusers(want)
	assertProduce("unreadable bootstrap leaves grants unchanged")
}
