package perf

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/go-softwarelab/common/pkg/to"
	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv/mockarcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/brc29"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/monitor"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/services"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wallet"
)

const (
	walletKeyHex     = "0000000000000000000000000000000000000000000000000000000000000001"
	senderKeyHex     = "0000000000000000000000000000000000000000000000000000000000000002"
	storageIDKey     = "perf-storage-identity-key"
	originator       = "perf"
	derivationPrefix = "Pg==" // base64 of the fixed derivation prefix byte
	seedBaseHeight   = uint32(800_000)
	seedChunkOutputs = 500
)

// stack is the fully-wired write-path stack the harness drives.
type stack struct {
	wallet       *wallet.Wallet
	provider     *storage.Provider
	arc          *mockarcade.Arcade
	ct           *mockarcade.ChainTracks
	mon          *monitor.Daemon
	recipientHex string

	closers []func()
}

func (s *stack) close(ctx context.Context) {
	if s.mon != nil {
		_ = s.mon.Stop()
	}
	for i := len(s.closers) - 1; i >= 0; i-- {
		s.closers[i]()
	}
	_ = ctx
}

// Run builds the stack, pre-mints the pool, drives the workers for
// warmup+duration, and returns the run result.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) (*RunResult, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	st, err := buildStack(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}
	defer st.close(ctx)

	// The auto-miner drives the in-process mock arcade's SSE; with a live
	// arcade the harness does not control the stream, so mining is disabled.
	if st.arc == nil {
		cfg.Mine = false
	}

	maxSeedHeight, err := seedPool(ctx, cfg, st, logger)
	if err != nil {
		return nil, fmt.Errorf("perf: seed pool: %w", err)
	}

	// Give the monitor's SSE subscriber a moment to connect before mining.
	if cfg.RunMonitor {
		time.Sleep(750 * time.Millisecond)
	}

	dest, err := throwawayScript()
	if err != nil {
		return nil, fmt.Errorf("perf: build destination script: %w", err)
	}

	coll := newCollector(cfg.Workers)
	var limiter *rate.Limiter
	if cfg.TargetTPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.TargetTPS), int(cfg.TargetTPS)+1)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	t0 := time.Now()
	timer := time.AfterFunc(cfg.Warmup+cfg.Duration, cancel)
	defer timer.Stop()

	var txidCh chan string
	var wgAux sync.WaitGroup
	if cfg.Mine {
		txidCh = make(chan string, cfg.Workers*8)
		wgAux.Add(1)
		go func() {
			defer wgAux.Done()
			runMiner(runCtx, st, coll, txidCh, maxSeedHeight+1)
		}()
	}
	// Pool value sampler.
	poolInterval := 10 * time.Second
	if step := (cfg.Warmup + cfg.Duration) / 8; step < poolInterval && step > 0 {
		poolInterval = step
	}
	wgAux.Add(1)
	go func() {
		defer wgAux.Done()
		runPoolSampler(runCtx, st, coll, poolInterval, t0)
	}()

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorker(runCtx, id, cfg, st, coll, limiter, dest, txidCh)
		}(i)
	}
	wg.Wait()
	cancel()
	wgAux.Wait()

	measureStart := t0.Add(cfg.Warmup)
	measureEnd := t0.Add(cfg.Warmup + cfg.Duration)
	result := snapshot(ctx, cfg, st, coll, measureStart, measureEnd)
	return result, nil
}

// buildStack wires mockarcade + oracle + headers + provider + wallet + monitor.
func buildStack(ctx context.Context, cfg Config, logger *slog.Logger) (*stack, error) {
	st := &stack{}

	// ChainTracks is always the in-process mock: it validates the harness's
	// synthetic seed proofs (and, in mock-arcade mode, the mined BUMPs).
	ct, closeCt := mockarcade.NewChainTracksServer()
	st.ct = ct
	st.closers = append(st.closers, closeCt)

	var arcadeURL string
	if cfg.RealArcadeURL != "" {
		arcadeURL = cfg.RealArcadeURL
	} else {
		arc, closeArc := mockarcade.NewArcadeServer()
		st.arc = arc
		st.closers = append(st.closers, closeArc)
		arcadeURL = arc.URL()
	}

	oracle := arcade.New(logger, nil, defs.Arcade{Enabled: true, URL: arcadeURL, EventsURL: arcadeURL})
	hdrs, err := headers.New(logger, defs.ChainTracks{Enabled: true, URL: ct.URL()})
	if err != nil {
		st.close(ctx)
		return nil, fmt.Errorf("perf: headers client: %w", err)
	}

	provider, closeProv, err := perfprovider.New(ctx, logger, cfg.perfproviderConfig(), oracle, hdrs)
	if err != nil {
		st.close(ctx)
		return nil, err
	}
	st.provider = provider
	st.closers = append(st.closers, func() { _ = closeProv(context.Background()) })

	if _, err := provider.Migrate(ctx, "perf", storageIDKey); err != nil {
		st.close(ctx)
		return nil, fmt.Errorf("perf: migrate provider: %w", err)
	}

	svc := services.New(logger, oracle, hdrs, defs.DefaultServicesConfig(cfg.Network))
	w, err := wallet.New(
		cfg.Network, walletKeyHex, provider,
		wallet.WithServices(svc),
		wallet.WithLogger(logger),
	)
	if err != nil {
		st.close(ctx)
		return nil, fmt.Errorf("perf: build wallet: %w", err)
	}
	st.wallet = w

	pub, err := w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, originator)
	if err != nil {
		st.close(ctx)
		return nil, fmt.Errorf("perf: wallet auth: %w", err)
	}
	st.recipientHex = pub.PublicKey.ToDERHex()

	if cfg.RunMonitor {
		mon, err := monitor.NewDaemon(logger, provider, hdrs, oracle,
			defs.DefaultMonitorConfig(), monitor.WithoutDistributedLock())
		if err != nil {
			st.close(ctx)
			return nil, fmt.Errorf("perf: build monitor: %w", err)
		}
		if err := mon.Start(ctx, nil); err != nil {
			st.close(ctx)
			return nil, fmt.Errorf("perf: start monitor: %w", err)
		}
		st.mon = mon
	}

	return st, nil
}

// seedPool pre-mints cfg.PoolSize coins of cfg.Denomination into the wallet's
// change basket via InternalizeAction of mined BRC-29 payments, in chunks. It
// returns the highest block height it registered.
func seedPool(ctx context.Context, cfg Config, st *stack, _ *slog.Logger) (uint32, error) {
	senderPriv, err := ec.PrivateKeyFromHex(senderKeyHex)
	if err != nil {
		return 0, fmt.Errorf("sender key: %w", err)
	}
	prefixBytes, err := base64.StdEncoding.DecodeString(derivationPrefix)
	if err != nil {
		return 0, err
	}

	remaining := cfg.PoolSize
	height := seedBaseHeight
	var suffixCounter uint64
	for remaining > 0 {
		n := seedChunkOutputs
		if remaining < n {
			n = remaining
		}
		atomicBEEF, root, _, suffixes, err := buildSeedBEEF(st.recipientHex, cfg.Denomination, n, suffixCounter, height)
		if err != nil {
			return 0, err
		}
		st.ct.RegisterHeader(height, root)

		outs := make([]sdk.InternalizeOutput, n)
		for i := 0; i < n; i++ {
			outs[i] = sdk.InternalizeOutput{
				OutputIndex: uint32(i), //nolint:gosec // i < seedChunkOutputs
				Protocol:    sdk.InternalizeProtocolWalletPayment,
				PaymentRemittance: &sdk.Payment{
					DerivationPrefix:  prefixBytes,
					DerivationSuffix:  suffixes[i],
					SenderIdentityKey: senderPriv.PubKey(),
				},
			}
		}
		if _, err := st.wallet.InternalizeAction(ctx, sdk.InternalizeActionArgs{
			Tx:          atomicBEEF,
			Outputs:     outs,
			Description: "perf seed",
		}, originator); err != nil {
			return 0, fmt.Errorf("internalize seed chunk: %w", err)
		}

		suffixCounter += uint64(n)
		remaining -= n
		height++
	}
	return height - 1, nil
}

// buildSeedBEEF builds a single-tx, single-leaf-proven atomic BEEF with n
// BRC-29 payment outputs of denom sats to recipientHex, each under a distinct
// derivation suffix so every coin has its own key/script. The single leaf IS
// the txid, so the computed merkle root equals the txid.
func buildSeedBEEF(recipientHex string, denom uint64, n int, suffixStart uint64, height uint32) (atomicBEEF []byte, root chainhash.Hash, txid string, suffixes [][]byte, err error) {
	tx := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = 0x11
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})

	suffixes = make([][]byte, n)
	for i := 0; i < n; i++ {
		sfx := make([]byte, 8)
		binary.BigEndian.PutUint64(sfx, suffixStart+uint64(i))
		suffixes[i] = sfx
		keyID := brc29.KeyID{
			DerivationPrefix: derivationPrefix,
			DerivationSuffix: base64.StdEncoding.EncodeToString(sfx),
		}
		ls, lerr := brc29.LockForCounterparty(brc29.PrivHex(senderKeyHex), keyID, brc29.PubHex(recipientHex))
		if lerr != nil {
			return nil, chainhash.Hash{}, "", nil, fmt.Errorf("lock seed output %d: %w", i, lerr)
		}
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: denom, LockingScript: ls})
	}

	txidHash := tx.TxID()
	trueVal := true
	mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: txidHash, Txid: &trueVal}},
	})
	if err := tx.AddMerkleProof(mp); err != nil {
		return nil, chainhash.Hash{}, "", nil, err
	}
	computedRoot, err := mp.ComputeRoot(txidHash)
	if err != nil {
		return nil, chainhash.Hash{}, "", nil, err
	}
	beef := transaction.NewBeefV2()
	if _, err := beef.MergeTransaction(tx); err != nil {
		return nil, chainhash.Hash{}, "", nil, err
	}
	atomic, err := beef.AtomicBytes(txidHash)
	if err != nil {
		return nil, chainhash.Hash{}, "", nil, err
	}
	return atomic, *computedRoot, txidHash.String(), suffixes, nil
}

// throwawayScript returns a fresh P2PKH locking script to use as the payment
// destination for every op.
func throwawayScript() ([]byte, error) {
	priv, err := ec.NewPrivateKey()
	if err != nil {
		return nil, err
	}
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), false)
	if err != nil {
		return nil, err
	}
	ls, err := p2pkh.Lock(addr)
	if err != nil {
		return nil, err
	}
	return ls.Bytes(), nil
}

// runWorker loops issuing ops until ctx is canceled.
func runWorker(ctx context.Context, id int, cfg Config, st *stack, coll *collector, limiter *rate.Limiter, dest []byte, txidCh chan<- string) {
	for {
		if ctx.Err() != nil {
			return
		}
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}
		txid, ok := doOp(ctx, id, cfg, st, coll, dest)
		if ok && txid != "" && txidCh != nil {
			select {
			case txidCh <- txid:
			default: // miner is behind; this tx simply stays unproven-spendable
			}
		}
	}
}

// doOp performs one write-path op with retry on contention/deadlock, records
// the sample + counters, and returns the txid on success.
func doOp(ctx context.Context, id int, cfg Config, st *stack, coll *collector, dest []byte) (string, bool) {
	coll.counters.attempted.Add(1)
	lastKind := errOther
	for attempt := 0; attempt <= cfg.MaxOpRetries; attempt++ {
		if ctx.Err() != nil {
			return "", false
		}
		txid, sample, kind := singleOp(ctx, cfg, st, dest)
		if kind == errNone {
			coll.record(id, sample)
			coll.counters.succeeded.Add(1)
			return txid, true
		}
		lastKind = kind
		if kind == errContention {
			coll.counters.contentionRetries.Add(1)
			backoff(ctx, attempt)
			continue
		}
		if kind == errDeadlock {
			coll.counters.deadlockRetries.Add(1)
			backoff(ctx, attempt)
			continue
		}
		break // non-retryable
	}
	// Exhausted or non-retryable.
	switch lastKind {
	case errContention:
		coll.counters.contentionFails.Add(1)
	case errDeadlock:
		coll.counters.deadlockFails.Add(1)
	default:
		coll.counters.otherErrors.Add(1)
	}
	return "", false
}

// singleOp performs exactly one attempt and times its phases.
func singleOp(ctx context.Context, cfg Config, st *stack, dest []byte) (txid string, s opSample, kind errKind) {
	out := sdk.CreateActionOutput{
		LockingScript:     dest,
		Satoshis:          cfg.PaymentSats,
		OutputDescription: "perf-payment",
	}

	if cfg.Mode == ModeSignAndProcess {
		args := sdk.CreateActionArgs{
			Description: "perf-payment",
			Outputs:     []sdk.CreateActionOutput{out},
			Options:     &sdk.CreateActionOptions{SignAndProcess: to.Ptr(true)},
		}
		t0 := time.Now()
		res, err := st.wallet.CreateAction(ctx, args, originator)
		dur := time.Since(t0)
		if err != nil {
			debugErr("create(single)", err)
			return "", opSample{}, classifyErr(err)
		}
		return res.Txid.String(), opSample{e2eNs: dur.Nanoseconds(), doneAt: time.Now()}, errNone
	}

	// Two-step.
	args := sdk.CreateActionArgs{
		Description: "perf-payment",
		Outputs:     []sdk.CreateActionOutput{out},
		Options:     &sdk.CreateActionOptions{SignAndProcess: to.Ptr(false)},
	}
	tc := time.Now()
	createRes, err := st.wallet.CreateAction(ctx, args, originator)
	createDur := time.Since(tc)
	if err != nil {
		debugErr("create(twostep)", err)
		return "", opSample{}, classifyErr(err)
	}
	if createRes.SignableTransaction == nil {
		debugErr("create(twostep)", fmt.Errorf("nil signable transaction"))
		return "", opSample{}, errOther
	}
	ts := time.Now()
	signRes, err := st.wallet.SignAction(ctx, sdk.SignActionArgs{
		Reference: createRes.SignableTransaction.Reference,
	}, originator)
	signDur := time.Since(ts)
	if err != nil {
		debugErr("sign(twostep)", err)
		return "", opSample{}, classifyErr(err)
	}
	return signRes.Txid.String(), opSample{
		createNs: createDur.Nanoseconds(),
		signNs:   signDur.Nanoseconds(),
		e2eNs:    (createDur + signDur).Nanoseconds(),
		doneAt:   time.Now(),
	}, errNone
}

type errKind int

const (
	errNone errKind = iota
	errContention
	errDeadlock
	errOther
)

// classifyErr buckets a write-path error for the contention counters.
func classifyErr(err error) errKind {
	if err == nil {
		return errNone
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "deadlock"), strings.Contains(msg, "serializ"),
		strings.Contains(msg, "40p01"), strings.Contains(msg, "40001"):
		return errDeadlock
	case strings.Contains(msg, "contention"), strings.Contains(msg, "conflict"),
		// Under concurrency a SKIP-LOCKED claim can transiently find every
		// candidate coin reserved by a peer and report "not enough funds":
		// that is claim contention, not genuine pool exhaustion — retry it.
		strings.Contains(msg, "not enough funds"), strings.Contains(msg, "insufficient"):
		return errContention
	default:
		return errOther
	}
}

func backoff(ctx context.Context, attempt int) {
	d := time.Duration(1+attempt) * time.Millisecond
	if d > 25*time.Millisecond {
		d = 25 * time.Millisecond
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// runMiner drains broadcast txids and matures each one by registering a
// single-leaf header (root == txid) and emitting a MINED status frame the
// monitor's SSE pipeline consumes.
func runMiner(ctx context.Context, st *stack, coll *collector, txidCh <-chan string, startHeight uint32) {
	height := startHeight
	trueVal := true
	for {
		select {
		case <-ctx.Done():
			return
		case txid, ok := <-txidCh:
			if !ok {
				return
			}
			h, err := chainhash.NewHashFromHex(txid)
			if err != nil {
				continue
			}
			mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{
				{{Offset: 0, Hash: h, Txid: &trueVal}},
			})
			root, err := mp.ComputeRoot(h)
			if err != nil {
				continue
			}
			st.ct.RegisterHeader(height, *root)
			st.arc.EmitStatus(txid, "MINED", map[string]any{
				"merklePath":  hex.EncodeToString(mp.Bytes()),
				"blockHeight": height,
			})
			coll.counters.minedEmitted.Add(1)
			height++
		}
	}
}

// runPoolSampler records spendable balance at a fixed interval.
func runPoolSampler(ctx context.Context, st *stack, coll *collector, interval time.Duration, t0 time.Time) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bal, err := st.wallet.Balance(ctx)
			if err == nil {
				coll.recordPool(poolSample{atSec: time.Since(t0).Seconds(), balanceSats: bal})
			}
		}
	}
}

// discardWriter is an io.Writer that drops everything (default silent logger).
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// debugErrCount rate-limits debugErr output.
var debugErrCount atomic.Int64

// debugErr prints the first few op errors to stderr when PERF_DEBUG is set. It
// is a diagnostic aid for wiring problems and is a no-op by default.
func debugErr(phase string, err error) {
	if os.Getenv("PERF_DEBUG") == "" {
		return
	}
	if debugErrCount.Add(1) <= 8 {
		fmt.Fprintf(os.Stderr, "PERF_DEBUG %s: %v\n", phase, err)
	}
}
