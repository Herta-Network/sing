# Polaris optimization alignment

This fork retains the `github.com/metacubex/sing` module path and the existing
MetaCubeX function signatures. The changes below were adapted on 2026-11-01 from
Polaris's modified SagerNet sing, with additional ownership and lifecycle fixes.
Original upstream copyright and license notices are retained. The bundled
`contrab/freelru` and `contrab/maphash` directories retain their own licenses.

Reference: Polaris commit `56d52fb44958ef3cc2a7fb841cd240f8206234b2`, directory
`pkg/sagernet/sing`; its `MODIFICATIONS.md` records the upstream provenance.
The local alignment started from `61531dcc87a1967c7ff8b213129b01fc5fe64080`.

## Scope and implementation map

This aligns the base-library transport and resource optimizations in Polaris's
modification log through 2026-10-29. It is not a wholesale replacement with the
newer SagerNet API. Protocol-library forks and activation in mihomo remain
separate integration steps.

| Capability | Implementation |
| --- | --- |
| Bounded, sharded UDP NAT and pooled packet records | `common/udpnat/session_cache.go`, `service.go`; configurable ceiling, default 65,536 sessions; nonblocking full queues and identity-safe eviction |
| Asynchronous UDP NAT and batch result reuse | `common/udpnat2/`; fixed-capacity synced/sharded freelru, preparation outside cache locks, queued-buffer cleanup and once-only close callbacks |
| Bounded replay protection | `common/replay/simple.go`; rejects new salts at capacity, preserves the full configured timeout for salts added near window boundaries |
| Authentication scan guard and candidate cache | `common/authscan/`; configurable capacity, permit release, generation remapping and lazy shard allocation |
| Low-allocation random integers | `common/random/crypto.go`; rejection sampling without `big.Int` |
| Bounded stream buffers and dynamic vector reads | `CopyWithOptions` / `CopyReadOptions` in `common/bufio`; read policy controls batch size, optional growth and per-buffer cap; waiter reconfiguration releases owned buffers |
| Linux splice resource control | `common/bufio/splice_linux.go`; CPU-scaled 4–64 pipe-pair slots, pooled-copy fallback, pre-write hook and dynamic `TransferSize` cap |
| Native UDP batch I/O | `common/bufio/packet_batch*.go`; Linux/NetBSD mmsg and Darwin msg_x, reusable vectors/results, partial-send handling and empty-datagram support |
| Packet wrappers and accounting | `common/bufio/{bind,nat,counter_packet_conn}*.go`, `common/network/counter.go`; preserve address conversion, wrapper capabilities, counters and buffer ownership |
| UoT framing and batch writes | `common/uot/`; reusable vectors, payload-length checks, consumed owned buffers on failure, short-frame write handling and frame-aligned reads |
| Buffer pooling | `common/buf/`; 72 KiB maximum pooled class, allocator-first `NewSize`, exact visible capacity |
| Address serialization | `common/metadata/`; scratch/buffer encode/decode and caller-provided native sockaddr storage |
| Domain rules and typed slice decoding | `common/domain/`, `common/varbin/`; stack matching, duplicate removal, bounded allocation and succinct-set validation |
| SOCKS handshake and packet paths | `protocol/socks/`; context cancellation during Dial/Bind handshakes, buffer serializers, payload byte counts and failure cleanup |

## Deliberate differences from the reference

- Go 1.20 remains supported. Newer language/library syntax is translated without
  upgrading the module's toolchain requirement.
- Existing MetaCubeX connection handlers, wrappers and exported copy functions
  remain available. `CopyWithOptions` expresses the transfer policy instead of
  replacing it with Polaris's newer `CopySession` API.
- `ReadPolicy` selects the buffered stream path. Without it, Linux splice can
  use `BeforeTransfer` and `TransferSize`; saturation falls back to pooled copy.
  `MaxBufferSize` limits stream allocation requests, not datagram payloads or
  buffers already owned by a protocol. A pool may round the actual allocation.
- Reader-side protocol overhead is not added again to the writer's headroom.
  Stream growth is opt-in; packet MTUs are preserved.
- Read-waiter scratch slices are borrowed until the next read, reinitialization
  or release. Payload ownership transfers to the caller. Close/release does not
  reclaim payloads already handed out.
- Cache eviction callbacks execute after unlocking. `udpnat2` preparation also
  runs outside the cache lock; its response writer is borrowed and may belong to
  a shared listener. Session close drains queued packets and invokes cleanup
  once, without closing that shared writer.
- Additional regressions cover timeout-window retention, truncation, short
  writes, partial transfers, empty packets, reentrant cleanup and cancellation.
  In particular, Darwin batch truncation is detected with an extra byte because
  the native batch API did not reliably report truncation in local tests.
- Unrelated upstream additions (platform discovery/control APIs, HTTP/SOCKS
  server handler API replacement, JSON/context/error utilities and toolchain
  upgrades) are outside this optimization port. Existing MetaCubeX behavior is
  retained there. The existing cached-prefix fix in `ChunkReader` is retained.

## Validation boundaries

Validation runs on macOS arm64 unless explicitly labeled otherwise:

- Full repository tests and focused `-race` tests, including `debug` and
  `with_low_memory` buffer ownership checks.
- Go 1.20.14 tests for changed packages, and `go vet` for those packages.
- Native Darwin TCP/UDP loopback tests for vector/batch I/O, connected and
  unconnected UDP, empty packets, truncation and counters.
- Cross-builds for Linux amd64/386, Windows amd64, iOS arm64, FreeBSD amd64 and
  NetBSD amd64; Linux-specific splice tests are compiled, not executed here.
- mihomo's affected packages compile with a temporary module replacement.
  Its checked-in dependency remains unchanged.

Full-repository vet still reports the pre-existing `common/cond.go:177`
`unsafe.Pointer` diagnostic. There is no Linux runtime, iOS-device RSS/throughput
measurement, or production deployment proof from these local checks.

## Batch write progress

The optional `PacketBatchWriterWithCount` and connected equivalent preserve the
old error-only APIs while returning a completed packet prefix on both success
and failure. Native mmsg/msg_x writers retain progress across short sends,
poll waits, timeouts and socket closure. Every input buffer is consumed,
including unsent tails; the result does not authorize retrying freed buffers.

Counted vector writes retain `net.Buffers` for standard stream sockets and join
buffers for other `io.Writer` implementations without bypassing protocol
wrappers. UoT maps accepted bytes to complete frames, excludes its lazy request
header, and closes/poisons the stream after partial-frame failure. Counter and
NAT adapters retain prefix accounting and address translation; empty datagrams
count as packets. CopyPacket uses scalar writes for opaque error-only batch
writers instead of guessing partial progress.

Regression coverage includes native partial-send/poll-error injection,
zero-length datagrams, scalar fallback, NAT translation, counter prefixes,
every UoT frame boundary, lazy-header failures and once-only buffer release.
