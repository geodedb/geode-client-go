//go:build integration

package geode

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// driverTestContainerInfo holds the container info for driver integration tests.
type driverTestContainerInfo struct {
	container testcontainers.Container
	dsn       string
}

// driverTestContainer holds the container for driver integration tests.
var (
	driverTestContainer *driverTestContainerInfo
	driverContainerMu   sync.Mutex
)

// getTestDSN returns the DSN from the shared Docker container when running
// with the integration build tag.
func getTestDSN(t *testing.T) string {
	driverContainerMu.Lock()
	defer driverContainerMu.Unlock()

	if driverTestContainer != nil {
		return driverTestContainer.dsn
	}

	// Start a new container
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "registry.gitlab.com/devnw/geode/geode:latest",
		ExposedPorts: []string{"3141/udp"},
		WaitingFor:   wait.ForLog("Server ready").WithStartupTimeout(60 * time.Second),
		Cmd:          []string{"serve", "--listen", "0.0.0.0:3141", "--log-level", "info"},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start Geode container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("Failed to get container host: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, "3141/udp")
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("Failed to get mapped port: %v", err)
	}

	dsn := fmt.Sprintf("%s:%s?insecure=true", host, mappedPort.Port())

	driverTestContainer = &driverTestContainerInfo{
		container: container,
		dsn:       dsn,
	}

	// Register cleanup
	t.Cleanup(func() {
		driverContainerMu.Lock()
		defer driverContainerMu.Unlock()
		if driverTestContainer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			driverTestContainer.container.Terminate(ctx)
			driverTestContainer = nil
		}
	})

	return dsn
}
