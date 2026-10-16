// Package e2e runs the real sylphy-server binary as a child process and
// exercises persistence the way an operator would: it kills the process with
// SIGKILL, stops it with SIGTERM, damages the data directory and restarts on
// the same directory. It is separate from test/integration, whose tests run
// the server in-process, because these tests build a binary, are slower, and
// need POSIX signals (they skip on Windows).
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nima-ca/sylphy/internal/persist"
)

// eq fails the test if got != want.
func eq[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// wInt runs a command and returns its integer reply.
func wInt(t *testing.T, c *redis.Client, args ...any) int64 {
	t.Helper()
	v, err := c.Do(context.Background(), args...).Int64()
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

// wText runs a command and returns its string or status reply.
func wText(t *testing.T, c *redis.Client, args ...any) string {
	t.Helper()
	v, err := c.Do(context.Background(), args...).Text()
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

// wErr runs a command and returns its error text, failing if it succeeds.
func wErr(t *testing.T, c *redis.Client, args ...any) string {
	t.Helper()
	err := c.Do(context.Background(), args...).Err()
	if err == nil {
		t.Fatalf("%v: expected an error reply", args)
	}
	return err.Error()
}

// eventually polls cond every 10 ms until it holds, failing the test at limit.
func eventually(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// e2eBinDir holds the compiled server binary. It is set (lazily) by the first
// test that needs the binary and removed by TestMain.
var e2eBinDir string

// TestMain exists only to delete the compiled binary after the run; the binary
// itself is built on first use (see e2eBinary).
func TestMain(m *testing.M) {
	code := m.Run()
	if e2eBinDir != "" {
		_ = os.RemoveAll(e2eBinDir)
	}
	os.Exit(code)
}

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// e2eBinary returns the path of the compiled sylphy-server, building it on the
// first call. It skips the calling test on Windows: the tests need POSIX
// signals (SIGTERM) and POSIX file semantics.
func e2eBinary(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("persistence E2E tests need POSIX signals and file semantics; not supported on Windows")
	}
	buildOnce.Do(func() { binPath, buildErr = buildServer() })
	if buildErr != nil {
		t.Fatalf("building sylphy-server: %v", buildErr)
	}
	return binPath
}

func buildServer() (string, error) {
	wd, err := os.Getwd() // go test runs with the package directory as cwd
	if err != nil {
		return "", err
	}
	root := filepath.Join(wd, "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("cannot find the module root from %s: %w", wd, err)
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		return "", fmt.Errorf("the go tool is needed to build the server: %w", err)
	}
	dir, err := os.MkdirTemp("", "sylphy-e2e-bin-")
	if err != nil {
		return "", err
	}
	e2eBinDir = dir
	out := filepath.Join(dir, "sylphy-server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, goBin, "build", "-o", out, "./cmd/sylphy-server")
	cmd.Dir = root
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%w\n%s", err, b)
	}
	return out, nil
}

// e2eEnv is the child's environment: ours without SYLPHY_*, so a developer's
// own settings cannot change what a test starts.
func e2eEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "SYLPHY_") {
			env = append(env, kv)
		}
	}
	return env
}

// syncBuffer is a bytes.Buffer safe for the exec copy goroutines and the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// proc is one sylphy-server child process.
type proc struct {
	t      *testing.T
	cmd    *exec.Cmd
	addr   string
	stdout syncBuffer
	stderr syncBuffer
	done   chan struct{} // closed when the process has exited and been reaped
	werr   error         // Wait's result; valid once done is closed
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()
	if err != nil {
		t.Fatal(err)
	}
	return port
}

// spawn starts the server with -addr 127.0.0.1:<free port> plus args. workdir
// is the child's working directory ("" inherits ours). The child is killed in
// t.Cleanup. It does not wait for readiness.
func spawn(t *testing.T, workdir string, args ...string) *proc {
	t.Helper()
	bin := e2eBinary(t)
	p := &proc{t: t, addr: "127.0.0.1:" + freePort(t), done: make(chan struct{})}
	p.cmd = exec.Command(bin, append([]string{"-addr", p.addr}, args...)...)
	p.cmd.Dir = workdir
	p.cmd.Env = e2eEnv()
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", bin, err)
	}
	go func() {
		p.werr = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(p.kill)
	return p
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// exitCode returns the exit status; call it only after done is closed.
func (p *proc) exitCode() int { return p.cmd.ProcessState.ExitCode() }

// logs renders the captured output for failure messages.
func (p *proc) logs() string {
	return fmt.Sprintf("--- stdout ---\n%s\n--- stderr ---\n%s", p.stdout.String(), p.stderr.String())
}

// kill is kill -9 (SIGKILL). It is idempotent.
func (p *proc) kill() {
	if p.exited() {
		return
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.t.Errorf("process %d did not exit after SIGKILL", p.cmd.Process.Pid)
	}
}

// term sends SIGTERM (graceful stop), waits up to timeout and returns the exit
// code.
func (p *proc) term(timeout time.Duration) int {
	p.t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatalf("SIGTERM: %v\n%s", err, p.logs())
	}
	select {
	case <-p.done:
	case <-time.After(timeout):
		p.kill()
		p.t.Fatalf("server still running %v after SIGTERM\n%s", timeout, p.logs())
	}
	return p.exitCode()
}

func pingOnce(addr string) bool {
	c := redis.NewClient(&redis.Options{
		Addr: addr, Protocol: 2, MaxRetries: -1, PoolSize: 1,
		DialTimeout: 200 * time.Millisecond, ReadTimeout: time.Second,
	})
	defer func() { _ = c.Close() }()
	return c.Ping(context.Background()).Err() == nil
}

// waitReady polls PING until the server answers. Recovery runs before the
// listener opens, so a successful PING means recovery has finished.
func (p *proc) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if p.exited() {
			return fmt.Errorf("exited before listening: %v", p.werr)
		}
		if pingOnce(p.addr) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("not ready before the deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startProc starts the server and waits until it answers PING. A port picked a
// moment earlier can be taken by someone else, so it retries a few times on
// "address already in use".
func startProc(t *testing.T, workdir string, args ...string) *proc {
	t.Helper()
	for range 3 {
		p := spawn(t, workdir, args...)
		err := p.waitReady(30 * time.Second)
		if err == nil {
			return p
		}
		p.kill()
		if strings.Contains(p.stderr.String(), "address already in use") {
			continue
		}
		t.Fatalf("server did not become ready: %v\n%s", err, p.logs())
	}
	t.Fatal("could not get a free port for the server after 3 attempts")
	return nil
}

// runToExit starts the server expecting it to exit by itself (a refused start)
// and returns its exit code and stderr.
func runToExit(t *testing.T, workdir string, args ...string) (code int, stderr string) {
	t.Helper()
	p := spawn(t, workdir, args...)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		p.kill()
		t.Fatalf("server neither started nor exited within 30s\n%s", p.logs())
	}
	return p.exitCode(), p.stderr.String()
}

func newE2EClient(addr string, pool int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr, Protocol: 2, MaxRetries: -1, PoolSize: pool,
		DialTimeout: 2 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
	})
}

func e2eClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := newE2EClient(addr, 8)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// aofArgs are the flags for an AOF server on dir with the given fsync policy.
func aofArgs(dir, fsync string, extra ...string) []string {
	return append([]string{"-persistence-mode", "aof", "-dir", dir, "-append-fsync", fsync}, extra...)
}

// waitUntil blocks until the wall clock reaches tm. It is for "the key's
// deadline has passed while the server was down", which is a property of the
// clock, not of the server.
func waitUntil(tm time.Time) {
	for {
		d := time.Until(tm)
		if d <= 0 {
			return
		}
		time.Sleep(min(d, 50*time.Millisecond))
	}
}

// e2eWaitIdle returns once no checkpoint is running. SAVE is refused while one
// is, and blocks until done otherwise, so the first SAVE that succeeds proves
// idleness (it runs one more checkpoint, which is harmless to every caller).
func e2eWaitIdle(t *testing.T, c *redis.Client) {
	t.Helper()
	eventually(t, 60*time.Second, "the running checkpoint to finish", func() bool {
		err := c.Do(context.Background(), "SAVE").Err()
		switch {
		case err == nil:
			return true
		case strings.Contains(err.Error(), "already in progress"):
			return false
		}
		t.Fatalf("SAVE: %v", err)
		return false
	})
}

// dirState maps every file in dir to its size and SHA-256, to prove that a
// refused start left the directory byte-for-byte untouched.
func dirState(t *testing.T, dir string) map[string]string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := make(map[string]string, len(ents))
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		m[e.Name()] = fmt.Sprintf("%d:%x", len(b), sha256.Sum256(b))
	}
	return m
}

// e2eLastSegment returns the path of the newest segment the manifest lists.
func e2eLastSegment(t *testing.T, dir string) string {
	t.Helper()
	m, found, err := persist.LoadManifest(persist.OSFS(), dir)
	if err != nil || !found || len(m.Segments) == 0 {
		t.Fatalf("manifest in %s: found=%v segments=%d err=%v", dir, found, len(m.Segments), err)
	}
	return filepath.Join(dir, m.Segments[len(m.Segments)-1])
}

// e2eAssertDirConsistent fails if dir holds anything the manifest does not
// list (a temp file, a stale base or segment).
func e2eAssertDirConsistent(t *testing.T, dir string) {
	t.Helper()
	m, found, err := persist.LoadManifest(persist.OSFS(), dir)
	if err != nil || !found {
		t.Fatalf("manifest in %s: found=%v err=%v", dir, found, err)
	}
	allowed := map[string]bool{persist.ManifestName: true}
	if m.Base.Name != "" {
		allowed[m.Base.Name] = true
	}
	for _, s := range m.Segments {
		allowed[s] = true
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if !allowed[e.Name()] {
			t.Errorf("%s holds %q, which the manifest does not list", dir, e.Name())
		}
	}
}

// e2eAssertNoSegments fails if dir holds an AOF segment (snapshot mode has none).
func e2eAssertNoSegments(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if _, ok := persist.ParseSegmentName(e.Name()); ok {
			t.Errorf("snapshot mode left a log segment: %s", e.Name())
		}
	}
}

// e2eScan decodes data as an AOF segment with the production record decoder.
// It returns the start offset of every complete record, the offset where the
// scan ended and the error that ended it (nil at a clean end).
func e2eScan(data []byte) (starts []int64, end int64, err error) {
	rd := persist.NewRecordReader(bytes.NewReader(data), persist.RecordLimits{})
	for {
		start := rd.Offset()
		if _, err := rd.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				return starts, start, nil
			}
			return starts, start, err
		}
		starts = append(starts, start)
	}
}

// e2eSetKeys returns the keys of the SET records currently readable in the
// segment at path (a torn tail is ignored). Commands without a rewrite hook are
// logged as the client spelled them, hence EqualFold.
func e2eSetKeys(t *testing.T, path string) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	rd := persist.NewRecordReader(bytes.NewReader(data), persist.RecordLimits{})
	for {
		argv, err := rd.Next()
		if err != nil {
			return keys
		}
		if len(argv) >= 2 && strings.EqualFold(string(argv[0]), "SET") {
			keys[string(argv[1])] = true
		}
	}
}

// Binary-safe test payloads: NUL, CR LF, high bytes and RESP type markers.
const (
	e2eBin    = "\x00\x01\r\n\xff$*+-:\r\nend"
	e2eBinKey = "bin\x00key\r\n\xff"
)

var (
	e2eDatasetKeys = []string{"str", "list", "hash", "set", "zset", "plain", e2eBinKey}
	e2eTTLKeys     = []string{"str", "list", "hash", "set", "zset"}
	// e2eZSet is in ascending score order, which is the order ZRANGE returns.
	// The scores are chosen to catch lossy float formatting.
	e2eZSet = []redis.Z{
		{Score: -2.25, Member: "b"},
		{Score: 0, Member: "zero"},
		{Score: 0.1, Member: e2eBin},
		{Score: 1.0 / 3.0, Member: "third"},
		{Score: 1.5, Member: "a"},
		{Score: 1e10, Member: "big"},
	}
)

// e2eDataset describes what writeDataset stored.
type e2eDataset struct {
	ttl     map[string]time.Duration // PTTL of each TTL key right after the writes
	shortAt time.Time                // when the short-TTL keys expire (zero: none written)
	extra   []string                 // keys that should exist besides the dataset's own
}

// writeDataset stores one key of every type, most with long TTLs. With
// withShort it also stores two keys (a string and a list) that expire about
// 1.5 s from now, so a test can let them expire while the server is down.
func writeDataset(t *testing.T, c *redis.Client, withShort bool) *e2eDataset {
	t.Helper()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	d := &e2eDataset{ttl: map[string]time.Duration{}}

	must(c.Set(ctx, "str", e2eBin, time.Hour).Err())
	must(c.RPush(ctx, "list", "a", "", e2eBin, "z").Err())
	must(c.PExpire(ctx, "list", 2*time.Hour).Err())
	must(c.HSet(ctx, "hash", "f1", "v1", e2eBin, e2eBin+"v", "empty", "").Err())
	must(c.PExpire(ctx, "hash", 3*time.Hour).Err())
	must(c.SAdd(ctx, "set", "m1", "m2", e2eBin, "").Err())
	must(c.PExpire(ctx, "set", 4*time.Hour).Err())
	must(c.ZAdd(ctx, "zset", e2eZSet...).Err())
	must(c.PExpire(ctx, "zset", 5*time.Hour).Err())
	must(c.Set(ctx, "plain", "no ttl", 0).Err())
	must(c.Set(ctx, e2eBinKey, e2eBin, 0).Err())

	if withShort {
		d.shortAt = time.Now().Add(1500 * time.Millisecond)
		must(c.Do(ctx, "SET", "short-str", "v", "PXAT", d.shortAt.UnixMilli()).Err())
		must(c.RPush(ctx, "short-list", "x").Err())
		must(c.PExpireAt(ctx, "short-list", d.shortAt).Err())
	}
	for _, k := range e2eTTLKeys {
		pttl := c.PTTL(ctx, k).Val()
		if pttl <= 0 {
			t.Fatalf("PTTL %s = %v right after setting a TTL", k, pttl)
		}
		d.ttl[k] = pttl
	}
	return d
}

// verify checks the whole dataset: exactly the expected keys, every value
// byte-for-byte, zset scores exactly, TTLs present and not larger than before
// (deadlines are absolute, so time spent down only shrinks them), and the
// short-TTL keys gone.
func (d *e2eDataset) verify(t *testing.T, c *redis.Client) {
	t.Helper()
	ctx := context.Background()

	want := slices.Concat(e2eDatasetKeys, d.extra)
	slices.Sort(want)
	got, err := c.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("keys: got %q, want %q", got, want)
	}
	for _, k := range []string{"short-str", "short-list"} {
		if n := c.Exists(ctx, k).Val(); n != 0 {
			t.Fatalf("%s survived its deadline", k)
		}
	}

	if v, err := c.Get(ctx, "str").Result(); err != nil || v != e2eBin {
		t.Fatalf("str: %q, %v", v, err)
	}
	if v, err := c.Get(ctx, "plain").Result(); err != nil || v != "no ttl" {
		t.Fatalf("plain: %q, %v", v, err)
	}
	if v, err := c.Get(ctx, e2eBinKey).Result(); err != nil || v != e2eBin {
		t.Fatalf("binary key: %q, %v", v, err)
	}
	if l, err := c.LRange(ctx, "list", 0, -1).Result(); err != nil || !slices.Equal(l, []string{"a", "", e2eBin, "z"}) {
		t.Fatalf("list: %q, %v", l, err)
	}
	h, err := c.HGetAll(ctx, "hash").Result()
	if err != nil || !maps.Equal(h, map[string]string{"f1": "v1", e2eBin: e2eBin + "v", "empty": ""}) {
		t.Fatalf("hash: %q, %v", h, err)
	}
	s, err := c.SMembers(ctx, "set").Result()
	slices.Sort(s)
	wantSet := []string{"", "m1", "m2", e2eBin}
	slices.Sort(wantSet)
	if err != nil || !slices.Equal(s, wantSet) {
		t.Fatalf("set: %q, %v", s, err)
	}
	z, err := c.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	if err != nil || len(z) != len(e2eZSet) {
		t.Fatalf("zset: %v, %v", z, err)
	}
	for i := range z {
		if z[i].Score != e2eZSet[i].Score || z[i].Member != e2eZSet[i].Member {
			t.Fatalf("zset[%d]: got %v, want %v", i, z[i], e2eZSet[i])
		}
	}

	for _, k := range e2eTTLKeys {
		after := c.PTTL(ctx, k).Val()
		if after <= 0 || after > d.ttl[k] {
			t.Fatalf("PTTL %s = %v after recovery, want 0 < ttl <= %v", k, after, d.ttl[k])
		}
	}
}

// e2eBulkSet stores prefix0..prefix{n-1} = val through pipelines.
func e2eBulkSet(t *testing.T, c *redis.Client, prefix string, n int, val string) {
	t.Helper()
	ctx := context.Background()
	for lo := 0; lo < n; lo += 1000 {
		pipe := c.Pipeline()
		for i := lo; i < min(lo+1000, n); i++ {
			pipe.Set(ctx, prefix+strconv.Itoa(i), val, 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("bulk set at %d: %v", lo, err)
		}
	}
}

// e2eBulkVerify checks what e2eBulkSet stored.
func e2eBulkVerify(t *testing.T, c *redis.Client, prefix string, n int, val string) {
	t.Helper()
	ctx := context.Background()
	for lo := 0; lo < n; lo += 1000 {
		pipe := c.Pipeline()
		var cmds []*redis.StringCmd
		for i := lo; i < min(lo+1000, n); i++ {
			cmds = append(cmds, pipe.Get(ctx, prefix+strconv.Itoa(i)))
		}
		_, _ = pipe.Exec(ctx) // each command's own error is inspected below
		for i, cmd := range cmds {
			if v, err := cmd.Result(); err != nil || v != val {
				t.Fatalf("%s%d: got %q, %v", prefix, lo+i, v, err)
			}
		}
	}
}

// e2eVerifyAcked checks that every acknowledged write is present.
func e2eVerifyAcked(t *testing.T, c *redis.Client, acked map[string]string) {
	t.Helper()
	ctx := context.Background()
	keys := make([]string, 0, len(acked))
	for k := range acked {
		keys = append(keys, k)
	}
	var bad []string
	for lo := 0; lo < len(keys); lo += 500 {
		chunk := keys[lo:min(lo+500, len(keys))]
		pipe := c.Pipeline()
		cmds := make([]*redis.StringCmd, len(chunk))
		for i, k := range chunk {
			cmds[i] = pipe.Get(ctx, k)
		}
		_, _ = pipe.Exec(ctx)
		for i, cmd := range cmds {
			if v, err := cmd.Result(); err != nil || v != acked[chunk[i]] {
				bad = append(bad, fmt.Sprintf("%s (got %q, %v)", chunk[i], v, err))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d acknowledged writes were lost or damaged, first: %v", len(bad), len(keys), bad[:min(10, len(bad))])
	}
}
