package message

import (
	"bytes"
	"strconv"

	"github.com/cockroachdb/errors"
)

// ErrCorruptedChunk indicates that a physical WAL record cannot belong to a
// trustworthy chunk run.
var ErrCorruptedChunk = errors.New("corrupted WAL payload chunk")

// IsChunkedPayload reports whether the message carries either reserved chunk
// marker. ChunkAssembler validates that both markers form a valid pair.
func IsChunkedPayload(msg BasicMessage) bool {
	return msg.Properties().Exist(messageChunkIndex) || msg.Properties().Exist(messageChunkTotal)
}

// AssembleChunks reassembles an index-ordered set of payload chunks
// from the durable WAL back into a single immutable message carrying
// the concatenated payload, the properties of the first chunk (with the chunk
// markers removed), and the first chunk's message ID -- which is the logical
// message ID returned to the append caller.
func AssembleChunks(chunks []ImmutableMessage) ImmutableMessage {
	first := chunks[0]

	totalLen := 0
	for _, c := range chunks {
		totalLen += len(c.IntoImmutableMessageProto().GetPayload())
	}
	payload := make([]byte, 0, totalLen)
	for _, c := range chunks {
		payload = append(payload, c.IntoImmutableMessageProto().GetPayload()...)
	}

	props := first.IntoImmutableMessageProto().GetProperties()
	assembledProps := make(map[string]string, len(props))
	for k, v := range props {
		assembledProps[k] = v
	}
	delete(assembledProps, messageChunkIndex)
	delete(assembledProps, messageChunkTotal)

	return NewImmutableMesasge(first.MessageID(), payload, assembledProps)
}

// ChunkAssembler reassembles physical WAL payload chunks back
// into complete messages. It is stateful and must be fed messages in WAL order
// via Push. Its zero value is ready to use.
//
// Chunks of one original message are correlated by their time tick -- unique
// per message on a pchannel and cloned onto every chunk -- so packs may
// interleave with other traffic in the log: no write-side coordination is
// required. Incomplete runs are not evicted based only on the number of
// concurrent runs: a run may still belong to a live writer, and dropping it
// would silently lose an acknowledged message when its remaining chunks arrive.
// A TimeTick is a safe barrier, however, so runs at or before it are discarded.
type ChunkAssembler struct {
	runs map[uint64]*chunkRun // keyed by the original message's time tick.
}

type chunkRun struct {
	total int
	slots map[int]ImmutableMessage // indexed sparsely so a corrupt `_ct` cannot force a large allocation.
}

// Push feeds one message (a chunk or a complete message) to the assembler.
// It returns:
//   - (assembled, true, nil) once the final missing chunk of a pack arrived and
//     the full message was reconstructed;
//   - (nil, true, nil) while chunks are being buffered (the caller must swallow
//     the message);
//   - (nil, false, nil) when msg is not a chunk (the caller processes it normally);
//   - a non-nil error when the chunk markers or an existing run are corrupted.
//
// A pack assembles from exactly `_ct` distinct indices under one time tick.
// A backend may persist a record and still surface its send as an error, so
// the producer's retry redelivers an identical record under a new message ID;
// such payload-identical duplicates are swallowed. The same index redelivered
// with different bytes, or a total mismatch inside one time tick, means the
// log does not carry what this assembler can trust: the caller must stop the
// scanner instead of advancing a checkpoint past it.
func (a *ChunkAssembler) Push(msg ImmutableMessage) (ImmutableMessage, bool, error) {
	idx, total, tt, chunked, err := parseChunkMetadata(msg)
	if !chunked {
		if msg.MessageType() == MessageTypeTimeTick || msg.MessageType() == MessageTypeMVCCBarrier {
			a.discardRunsAtOrBefore(msg.TimeTick())
		}
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if a.runs == nil {
		a.runs = make(map[uint64]*chunkRun)
	}

	run := a.runs[tt]
	if run == nil {
		if idx != 0 {
			// A middle chunk whose head never arrived: nothing joinable.
			return nil, true, nil
		}
		run = &chunkRun{total: total, slots: make(map[int]ImmutableMessage)}
		a.runs[tt] = run
	} else if run.total != total {
		a.deleteRun(tt)
		return nil, true, errors.Wrapf(ErrCorruptedChunk,
			"time tick %d changed total from %d to %d", tt, run.total, total)
	} else if previous, ok := run.slots[idx]; ok {
		if !sameChunkBytes(previous, msg) {
			a.deleteRun(tt)
			return nil, true, errors.Wrapf(ErrCorruptedChunk,
				"time tick %d index %d was rewritten with different payload bytes", tt, idx)
		}
		// Byte-identical: a persisted-but-unacked chunk rewritten by the
		// producer's retry. Keep the later observation: chunks are appended
		// sequentially within one run, so the last rewrite before the next
		// chunk carries the message ID and properties of the successful append.
		run.slots[idx] = msg
		return nil, true, nil
	}

	run.slots[idx] = msg
	if len(run.slots) < total {
		return nil, true, nil
	}
	chunks := make([]ImmutableMessage, total)
	for i := 0; i < total; i++ {
		chunks[i] = run.slots[i]
	}
	assembled := AssembleChunks(chunks)
	a.deleteRun(tt)
	return assembled, true, nil
}

func parseChunkMetadata(msg BasicMessage) (idx int, total int, tt uint64, chunked bool, err error) {
	idxValue, hasIndex := msg.Properties().Get(messageChunkIndex)
	totalValue, hasTotal := msg.Properties().Get(messageChunkTotal)
	if !hasIndex && !hasTotal {
		return 0, 0, 0, false, nil
	}
	if !hasIndex || !hasTotal {
		return 0, 0, 0, true, errors.Wrapf(ErrCorruptedChunk,
			"chunk markers must appear together: index=%q total=%q", idxValue, totalValue)
	}
	idx, idxErr := parseCanonicalNonNegativeInt(idxValue)
	total, totalErr := parseCanonicalNonNegativeInt(totalValue)
	if idxErr != nil || totalErr != nil || total < 2 || idx < 0 || idx >= total {
		return 0, 0, 0, true, errors.Wrapf(ErrCorruptedChunk,
			"invalid chunk markers: index=%q total=%q", idxValue, totalValue)
	}

	ttValue, hasTimeTick := msg.Properties().Get(messageTimeTick)
	parsedTimeTick, ttErr := DecodeUint64(ttValue)
	if !hasTimeTick || ttErr != nil || EncodeUint64(parsedTimeTick) != ttValue {
		return 0, 0, 0, true, errors.Wrapf(ErrCorruptedChunk,
			"chunk time tick is missing or invalid: %q", ttValue)
	}
	return idx, total, parsedTimeTick, true, nil
}

func parseCanonicalNonNegativeInt(value string) (int, error) {
	parsed, err := strconv.ParseUint(value, 10, strconv.IntSize)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, ErrCorruptedChunk
	}
	return int(parsed), nil
}

func (a *ChunkAssembler) discardRunsAtOrBefore(timeTick uint64) {
	for runTimeTick := range a.runs {
		if runTimeTick <= timeTick {
			a.deleteRun(runTimeTick)
		}
	}
}

// AdvanceTimeTick discards incomplete runs that can no longer complete before
// the observed logical TimeTick barrier. It is needed when a legacy TimeTick's
// message type becomes visible only after v0 conversion.
func (a *ChunkAssembler) AdvanceTimeTick(timeTick uint64) {
	a.discardRunsAtOrBefore(timeTick)
}

func (a *ChunkAssembler) deleteRun(tt uint64) {
	delete(a.runs, tt)
}

// sameChunkBytes reports whether two observations of one chunk position carry
// identical payload bytes. A retry keeps the payload but may change its message
// ID and attempt-specific properties such as trace context. Time tick, index,
// and total already identify the slot, so payload equality identifies a safe
// rewrite and Push keeps the later complete observation.
func sameChunkBytes(a, b ImmutableMessage) bool {
	return bytes.Equal(a.IntoImmutableMessageProto().GetPayload(), b.IntoImmutableMessageProto().GetPayload())
}
