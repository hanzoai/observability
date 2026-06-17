// Command observability is a Hanzo Base-native Go service binary: a typed ZAP
// capability-RPC backend for the Langfuse-style observability data model
// (traces, observations, sessions, scores, score-configs, events), built on
// Hanzo Base (embedded SQLite + plugins). It replaces 8 console in-process tRPC
// routers with native capability RPC.
//
// Architecture (the reference pattern the other migration service-binaries
// follow):
//
//	base.New()                    → Base app: embedded SQLite, hooks, migrations
//	  ├── vault (optional)        → per-org encrypted SQLite shard (KEK)
//	  ├── zap.MustRegister        → generic ORM transport (msgType 100–103)
//	  └── server.Register(node)   → THIS service's typed router (msgType 203)
//	apis.NewRouter(app)           → sidecar HTTP (health/metrics), NOT app data
//	app.Start()                   → serves HTTP :8090 + ZAP :9992
//
// The .zap schema (proto/) is the source of truth; gen/ is its Go projection.
package main

import (
	"context"
	"crypto/rand"
	"log"
	"os"

	"github.com/hanzoai/base"
	"github.com/hanzoai/base/core"
	"github.com/hanzoai/base/plugins/vault"
	"github.com/hanzoai/base/tools/hook"
	luxlog "github.com/luxfi/log"
	zaplib "github.com/luxfi/zap"
	zcap "github.com/zap-proto/go/cap"

	"github.com/hanzoai/observability/server"
)

func main() {
	app := base.New()

	var zapAddr string
	app.RootCmd.PersistentFlags().StringVar(&zapAddr, "zap", envOr("ZAP_ADDR", "127.0.0.1:9992"),
		"address for the typed ZAP capability-RPC listener")

	var defaultOrg string
	app.RootCmd.PersistentFlags().StringVar(&defaultOrg, "org", envOr("OBSERVABILITY_ORG", "default"),
		"default organization scope when a capability carries no org binding")

	var vaultDir string
	app.RootCmd.PersistentFlags().StringVar(&vaultDir, "vaultDir", os.Getenv("VAULT_DIR"),
		"directory for per-org encrypted SQLite shards (enables the vault plugin)")

	app.RootCmd.ParseFlags(os.Args[1:])

	// Optional: per-org encrypted SQLite backing via the vault plugin. Enabled
	// only when --vaultDir is set so local dev stays single-file SQLite. The
	// master KEK comes from KMS in production; a process-ephemeral key is used
	// when VAULT_MASTER_KEY is unset (dev only — shards won't persist across
	// restarts, which is correct for throwaway dev data).
	if vaultDir != "" {
		vault.MustRegister(app, vault.Config{
			Enabled:   true,
			DataDir:   vaultDir,
			OrgID:     defaultOrg,
			MasterKey: masterKey(),
		})
	}

	// Provision the observability collections (trace/observation/session/score/
	// score-config) at bootstrap. Idempotent; no env-seeded rows (OLTP tables).
	server.RegisterCollections(app)

	// Stand up the typed ZAP router alongside Base's serve lifecycle. A dedicated
	// luxfi/zap node carries the capability RPC (NoDiscovery: direct dial only —
	// service discovery is the gateway's job, not mDNS here).
	logger := luxlog.New("component", "observability")
	node := zaplib.NewNode(zaplib.NodeConfig{
		NodeID:      "observability",
		Port:        portOf(zapAddr),
		NoDiscovery: true,
	})

	// Verifier: bootstrap (ed25519, no issuer registry → Kind+Permissions
	// enforced, signature step skipped). Wire IssuerKey to the IAM pubkey
	// registry to enable full cryptographic verification.
	srv := server.NewServer(app, logger, defaultOrg, zcap.Verifier{})
	srv.Register(node)

	app.OnServe().Bind(&hook.Handler[*core.ServeEvent]{
		Id: "observabilityZapNode",
		Func: func(e *core.ServeEvent) error {
			if err := e.Next(); err != nil {
				return err
			}
			if err := node.Start(); err != nil {
				return err
			}
			logger.Info("observability ZAP router listening", "addr", zapAddr, "msgType", server.MsgTypeRouterBase)
			return nil
		},
	})
	app.OnTerminate().Bind(&hook.Handler[*core.TerminateEvent]{
		Id: "observabilityZapNodeStop",
		Func: func(e *core.TerminateEvent) error {
			node.Stop()
			return e.Next()
		},
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}

// masterKey returns the 32-byte vault master KEK: from VAULT_MASTER_KEY (hex or
// raw 32 bytes) in production, else a process-ephemeral random key for dev.
func masterKey() []byte {
	if v := os.Getenv("VAULT_MASTER_KEY"); len(v) >= 32 {
		return []byte(v)[:32]
	}
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	return k
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// portOf extracts the port from a host:port address, defaulting to 9992.
func portOf(addr string) int {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			p := 0
			for _, c := range addr[i+1:] {
				if c < '0' || c > '9' {
					return 9992
				}
				p = p*10 + int(c-'0')
			}
			if p == 0 {
				return 9992
			}
			return p
		}
	}
	return 9992
}

var _ = context.Background // reserved for future graceful-shutdown wiring
