package idgen

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/coderyw/goeonid/v2/eonid/internal/log"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestHashTagKeys(t *testing.T) {
	if option.NewDefaultIdGeneratorOptions("test", "", 0).DisableHashTag {
		t.Fatal("default must enable hash tag")
	}
	for _, disabled := range []bool{false, true} {
		prefix := "{eonId}"
		if disabled {
			prefix = "eonId"
		}
		for _, center := range []string{"", "b_user"} {
			wr := NewWorkerRegistry("test", center, 1023, nil, 0, disabled).(*workerRegistry)
			parent := prefix
			if center != "" {
				parent += ":" + center
			}
			parent += ":worker-id"
			if got := wr.getWorkerParentKey(); got != parent {
				t.Fatalf("parent: got %q, want %q", got, parent)
			}
			if got := wr.getWorkerKey(123); got != parent+":123" {
				t.Fatalf("worker: got %q", got)
			}
		}
	}
}

type renewalCapture struct {
	batches     [][]string
	pipelines   [][]redis.Cmder
	directCalls int
	failures    map[string]error
	missing     map[string]bool
}

func (h *renewalCapture) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *renewalCapture) ProcessPipelineHook(_ redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(_ context.Context, cmds []redis.Cmder) error {
		h.pipelines = append(h.pipelines, cmds)
		var firstErr error
		for _, cmd := range cmds {
			if err := h.process(cmd); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}
func (h *renewalCapture) ProcessHook(_ redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		h.directCalls++
		return h.process(cmd)
	}
}
func (h *renewalCapture) process(cmd redis.Cmder) error {
	args := cmd.Args()
	var keys []string
	if cmd.Name() == "expire" {
		keys = []string{args[1].(string)}
	} else {
		keys = make([]string, args[2].(int))
		for i := range keys {
			keys[i] = args[3+i].(string)
		}
	}
	h.batches = append(h.batches, keys)
	if err := h.failures[keys[0]]; err != nil {
		cmd.SetErr(err)
		return err
	}
	if cmd.Name() == "expire" {
		cmd.(*redis.BoolCmd).SetVal(!h.missing[keys[0]])
	} else {
		renewed := int64(len(keys))
		if h.missing[keys[0]] {
			renewed = 0
		}
		cmd.(*redis.Cmd).SetVal(renewed)
	}
	return nil
}

func TestHashTagRenewalBatches(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "unused:0"})
	defer client.Close()
	capture := &renewalCapture{}
	client.AddHook(capture)
	rm := &renewalMgr{redisClient: client, renewalNum: 100, renewalLua: redis.NewScript("return #KEYS")}
	expected := make(map[string]bool)
	for _, disabled := range []bool{false, true} {
		for i := 0; i < 101; i++ {
			wr := NewWorkerRegistry("test", fmt.Sprint(i), 1023, client, 0, disabled).(*workerRegistry)
			wr.workerKey = wr.getWorkerKey(i)
			rm.addToRenewal(wr)
			expected[wr.workerKey] = disabled
		}
	}
	rm.startRenewal()
	seen := make(map[string]bool)
	for _, batch := range capture.batches {
		if len(batch) > 100 {
			t.Fatal("batch limit exceeded")
		}
		for _, key := range batch {
			disabled, ok := expected[key]
			if !ok || seen[key] {
				t.Fatalf("unexpected or duplicated key %s", key)
			}
			if disabled && len(batch) != 1 {
				t.Fatalf("legacy key in multi-key Lua: %v", batch)
			}
			seen[key] = true
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("renewed %d of %d keys", len(seen), len(expected))
	}
	if len(capture.batches) != 103 {
		t.Fatalf("expected 101 single-key and 2 tagged batches, got %d", len(capture.batches))
	}
	if capture.directCalls != 2 || len(capture.pipelines) != 2 {
		t.Fatalf("want 2 tagged calls and 2 legacy pipelines, got %d and %d", capture.directCalls, len(capture.pipelines))
	}
	for i, cmds := range capture.pipelines {
		want := 100
		if i == 1 {
			want = 1
		}
		if len(cmds) != want {
			t.Fatalf("pipeline %d has %d commands, want %d", i, len(cmds), want)
		}
		for _, cmd := range cmds {
			if cmd.Name() != "expire" || len(cmd.Args()) != 3 || cmd.Args()[2] != int64(workerExpireSec) {
				t.Fatalf("expected single-key EXPIRE with lease TTL, got %v", cmd.Args())
			}
		}
	}
}

func TestHashTagPipelinePartialFailure(t *testing.T) {
	core, observed := observer.New(zap.ErrorLevel)
	original := log.Logger
	log.Logger = zap.New(core)
	t.Cleanup(func() { log.Logger = original })
	client := redis.NewClient(&redis.Options{Addr: "unused:0"})
	defer client.Close()
	capture := &renewalCapture{
		failures: map[string]error{"failed-1": errors.New("first failure"), "failed-2": errors.New("second failure")},
		missing:  map[string]bool{"expired": true},
	}
	client.AddHook(capture)
	rm := &renewalMgr{redisClient: client, renewalLua: redis.NewScript("return #KEYS")}
	rm.doPipelineRenewal(nil)
	if len(capture.pipelines) != 0 {
		t.Fatal("empty input executed a pipeline")
	}
	rm.doPipelineRenewal([]string{"failed-1", "healthy", "failed-2", "expired"})
	if len(capture.batches) != 4 {
		t.Fatal("partial failure skipped later commands")
	}
	for _, key := range []string{"failed-1", "failed-2", "expired"} {
		if observed.FilterField(zap.String("key", key)).Len() != 1 {
			t.Fatalf("missing per-key failure for %s", key)
		}
	}
	if observed.FilterField(zap.String("key", "healthy")).Len() != 0 {
		t.Fatal("healthy key reported as failed")
	}
}
