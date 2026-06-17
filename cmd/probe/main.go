// Command probe exercises a LIVE observability service over ZAP — the
// out-of-process smoke test. It mints a synthetic CapKindIAMSession capability
// (all read+write bits), connects to the service at --addr, writes a score
// config, reads it back, lists configs, and runs a pipelined
// traceById→observationById chain. Exit 0 on success, non-zero on any failure.
//
//	go run ./cmd/probe --addr 127.0.0.1:9992 --peer observability
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	zaplib "github.com/luxfi/zap"

	gen "github.com/hanzoai/observability/gen"
	"github.com/hanzoai/observability/server"
)

const probeOrg = "default"

func main() {
	addr := flag.String("addr", "127.0.0.1:9992", "service ZAP address")
	peer := flag.String("peer", "observability", "service ZAP node id")
	flag.Parse()

	if err := run(*addr, *peer); err != nil {
		fmt.Fprintln(os.Stderr, "PROBE FAILED:", err)
		os.Exit(1)
	}
	fmt.Println("PROBE OK")
}

func run(addr, peer string) error {
	// Full-privilege cap for the smoke test (every read+write bit set).
	capBuf, err := server.SyntheticCap(^uint64(0))
	if err != nil {
		return fmt.Errorf("mint cap: %w", err)
	}

	clientN := 0
	mkClient := func(log *[]server.SendEvent) (*server.Client, func(), error) {
		clientN++
		node := zaplib.NewNode(zaplib.NodeConfig{
			NodeID:      fmt.Sprintf("obs-probe-%d-%d", os.Getpid(), clientN),
			Port:        0, // OS-assigned ephemeral port
			NoDiscovery: true,
		})
		if err := node.Start(); err != nil {
			return nil, nil, err
		}
		c, err := server.Dial(node, addr, peer, capBuf)
		if err != nil {
			node.Stop()
			return nil, nil, err
		}
		if log != nil {
			c.WithSendLog(log)
		}
		return c, node.Stop, nil
	}

	var log []server.SendEvent
	cli, stop1, err := mkClient(&log)
	if err != nil {
		return err
	}
	defer stop1()
	dep, stop2, err := mkClient(&log)
	if err != nil {
		return err
	}
	defer stop2()

	time.Sleep(200 * time.Millisecond) // handshake settle

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create a score config (honest OLTP write).
	cfg, err := cli.ScoreConfigCreate(ctx, gen.ScoreConfigWriteParamsInput{
		ProjectId: probeOrg, Name: "probe-quality", DataType: "NUMERIC",
		HasMin: true, MinValue: 0, HasMax: true, MaxValue: 1,
	})
	if err != nil {
		return fmt.Errorf("ScoreConfigCreate: %w", err)
	}
	fmt.Printf("ScoreConfigCreate: id=%q name=%q\n", cfg.Id(), cfg.Name())

	// 2. Read it back by id.
	got, err := cli.ScoreConfigById(ctx, gen.ScoreConfigByIdParamsInput{ProjectId: probeOrg, Id: cfg.Id()})
	if err != nil {
		return fmt.Errorf("ScoreConfigById: %w", err)
	}
	if !got.Present() || got.Name() != "probe-quality" {
		return fmt.Errorf("ScoreConfigById mismatch: present=%v name=%q", got.Present(), got.Name())
	}
	fmt.Printf("ScoreConfigById: present=%v name=%q dataType=%q\n", got.Present(), got.Name(), got.DataType())

	// 3. List configs.
	list, err := cli.ScoreConfigAll(ctx, gen.ScoreConfigAllParamsInput{ProjectId: probeOrg})
	if err != nil {
		return fmt.Errorf("ScoreConfigAll: %w", err)
	}
	fmt.Printf("ScoreConfigAll: total=%d items=%d\n", list.TotalCount(), list.Items().Len())

	// 4. Pipelined traceById → observationById (no rows yet → present=false, but
	//    the pipelining mechanics are what we prove).
	log = log[:0]
	tr, ob, err := cli.PipelineTraceObservation(ctx, dep,
		gen.TraceByIdParamsInput{ProjectId: probeOrg, TraceId: "t-probe"},
		gen.ObservationByIdParamsInput{ProjectId: probeOrg, ObservationId: "o-probe", TraceId: "t-probe"},
	)
	if err != nil {
		return fmt.Errorf("PipelineTraceObservation: %w", err)
	}
	fmt.Printf("Pipeline: trace.present=%v observation.present=%v\n", tr.Present(), ob.Present())
	fmt.Printf("Pipeline send log: %s\n", fmtLog(log))

	sends, firstRecv := 0, -1
	for i, e := range log {
		if e.Kind == "recv" {
			firstRecv = i
			break
		}
		sends++
	}
	if firstRecv == -1 || sends < 2 {
		return fmt.Errorf("pipelining not observed: %d sends before first recv", sends)
	}
	fmt.Printf("Pipelining verified: %d calls in flight before the first answer\n", sends)
	return nil
}

func fmtLog(log []server.SendEvent) string {
	out := ""
	for _, e := range log {
		m := "traceById"
		if e.Method == server.MethodObservationById {
			m = "observationById"
		}
		out += fmt.Sprintf("%s(p=%d,t=%d) ", m, e.PromiseID, e.Target)
	}
	return out
}
