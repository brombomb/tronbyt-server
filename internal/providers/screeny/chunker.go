package screeny

import (
	"errors"
	"fmt"
	"image"
	"time"

	"tronbyt-server/internal/providers"

	"github.com/tronbyt/go-libwebp/webp"
)

// DefaultFPS is 30 frames per second.
const DefaultFPS = 30

// EncodeFramesToWebP encodes a sequence of RGBA images into an animated WebP binary.
func EncodeFramesToWebP(frames []*image.RGBA, frameDuration time.Duration) ([]byte, error) {
	if len(frames) == 0 {
		return nil, errors.New("cannot encode empty frame slice")
	}

	bounds := frames[0].Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	encoder, err := webp.NewAnimationEncoder(width, height, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("initializing webp animation encoder: %w", err)
	}
	defer encoder.Close()

	config, err := webp.ConfigLosslessPreset(6)
	if err != nil {
		return nil, fmt.Errorf("creating lossless config: %w", err)
	}

	for _, frame := range frames {
		if err := encoder.AddFrame(frame, frameDuration, config); err != nil {
			return nil, fmt.Errorf("adding frame to webp animation: %w", err)
		}
	}

	assembled, err := encoder.Assemble()
	if err != nil {
		return nil, fmt.Errorf("assembling webp animation: %w", err)
	}

	return assembled, nil
}

// EncodeFramesToWebPWithDuration encodes a sequence of RGBA images into an animated WebP binary
// whose frame timestamps span targetDurationMs evenly, with an optional holdLastFrameMs cushion
// added to the final frame so hardware decoders (ESP32 and Tronberry) never hit EOF and reset
// to frame 0 before dwell_secs expires.
func EncodeFramesToWebPWithDuration(frames []*image.RGBA, targetDurationMs int, holdLastFrameMs int) ([]byte, error) {
	if len(frames) == 0 {
		return nil, errors.New("cannot encode empty frame slice")
	}

	bounds := frames[0].Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	encoder, err := webp.NewAnimationEncoder(width, height, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("initializing webp animation encoder: %w", err)
	}
	defer encoder.Close()

	config, err := webp.ConfigLosslessPreset(6)
	if err != nil {
		return nil, fmt.Errorf("creating lossless config: %w", err)
	}

	n := len(frames)
	if targetDurationMs <= 0 {
		targetDurationMs = n * (1000 / DefaultFPS)
	}

	for i, frame := range frames {
		tStart := (i * targetDurationMs) / n
		tEnd := ((i + 1) * targetDurationMs) / n
		durMs := max(1, tEnd-tStart)
		if i == n-1 && n > 1 && holdLastFrameMs > 0 {
			durMs += holdLastFrameMs
		}
		if err := encoder.AddFrame(frame, time.Duration(durMs)*time.Millisecond, config); err != nil {
			return nil, fmt.Errorf("adding frame to webp animation: %w", err)
		}
	}

	assembled, err := encoder.Assemble()
	if err != nil {
		return nil, fmt.Errorf("assembling webp animation: %w", err)
	}

	return assembled, nil
}

// SplitFramesIntoChunks partitions sequential frames into animated WebP chunks,
// each fitting under maxChunkSize bytes and maxChunkDurationMs with exact playback DurationMs.
func SplitFramesIntoChunks(frames []*image.RGBA, fps int, maxChunkSize int, maxChunkDurationMs int) ([]providers.RenderChunk, error) {
	return SplitFramesIntoChunksWithDuration(frames, fps, maxChunkSize, maxChunkDurationMs, 0)
}

// SplitFramesIntoChunksWithDuration partitions sequential frames into animated WebP chunks.
// When targetDurationMs > 0, frame timestamps are paced across exact integer-second dwell windows
// with a final-frame hold cushion so hardware decoders never loop back to frame 0 mid-dwell.
func SplitFramesIntoChunksWithDuration(frames []*image.RGBA, fps int, maxChunkSize int, maxChunkDurationMs int, targetDurationMs int) ([]providers.RenderChunk, error) {
	if len(frames) == 0 {
		return nil, errors.New("no frames to split")
	}

	if fps <= 0 {
		fps = DefaultFPS
	}

	frameDurationMs := 1000 / fps
	if frameDurationMs <= 0 {
		frameDurationMs = 1
	}
	frameDuration := time.Duration(frameDurationMs) * time.Millisecond

	totalFrames := len(frames)
	totalSecs := 0
	holdMs := 0
	if targetDurationMs > 0 {
		totalSecs = max(1, (targetDurationMs+500)/1000)
		if targetDurationMs >= 5000 {
			holdMs = 250
		}
	}

	encodeRange := func(slice []*image.RGBA, durMs int, hold int) ([]byte, error) {
		if targetDurationMs > 0 {
			return EncodeFramesToWebPWithDuration(slice, durMs, hold)
		}
		return EncodeFramesToWebP(slice, frameDuration)
	}

	fullDurationMs := totalFrames * frameDurationMs
	if totalSecs > 0 {
		fullDurationMs = totalSecs * 1000
	}

	// 1. If maxChunkSize <= 0, don't chunk: encode everything into a single chunk.
	if maxChunkSize <= 0 {
		b, err := encodeRange(frames, fullDurationMs, holdMs)
		if err != nil {
			return nil, err
		}
		return []providers.RenderChunk{
			{
				WebPBytes:     b,
				DurationMs:    fullDurationMs,
				FullWebPBytes: b,
			},
		}, nil
	}

	maxFramesPerChunk := 0
	if maxChunkDurationMs > 0 {
		maxFramesPerChunk = (maxChunkDurationMs * fps) / 1000
	}

	// 2. Try encoding the entire animation in one chunk first if it fits within bounds.
	allBytes, err := encodeRange(frames, fullDurationMs, holdMs)
	if err == nil && len(allBytes) <= maxChunkSize && (maxFramesPerChunk <= 0 || totalFrames <= maxFramesPerChunk) {
		return []providers.RenderChunk{
			{
				WebPBytes:     allBytes,
				DurationMs:    fullDurationMs,
				FullWebPBytes: allBytes,
			},
		}, nil
	}

	// 3. Multi-chunk splitting:
	// Balance frame counts across chunks when targetDurationMs > 0 and maxFramesPerChunk is unset,
	// so e.g. a 180 KB animation under a 160 KB cap splits evenly into two ~90 KB chunks.
	if targetDurationMs > 0 && maxFramesPerChunk <= 0 && len(allBytes) > maxChunkSize {
		numChunksEst := max(2, (len(allBytes)+maxChunkSize-1)/maxChunkSize)
		maxFramesPerChunk = (totalFrames + numChunksEst - 1) / numChunksEst
	}

	var chunks []providers.RenderChunk
	start := 0
	remainingSecs := totalSecs

	chunkDurationFor := func(chunkStart, chunkEnd int) int {
		chunkFrames := chunkEnd - chunkStart
		if totalSecs <= 0 {
			return chunkFrames * frameDurationMs
		}
		remFrames := totalFrames - chunkStart
		if chunkEnd >= totalFrames || remainingSecs <= 1 || remFrames <= 0 {
			return max(1, remainingSecs) * 1000
		}
		secs := max(1, (chunkFrames*remainingSecs+remFrames/2)/remFrames)
		if secs >= remainingSecs {
			secs = max(1, remainingSecs-1)
		}
		return secs * 1000
	}

	for start < totalFrames {
		maxEnd := totalFrames
		if maxFramesPerChunk > 0 && start+maxFramesPerChunk < totalFrames {
			maxEnd = start + maxFramesPerChunk
		}

		// Fast path: test if maxEnd candidate fits under maxChunkSize in one step
		candidateAll := frames[start:maxEnd]
		candDurMs := chunkDurationFor(start, maxEnd)
		encodedAll, err := encodeRange(candidateAll, candDurMs, holdMs)
		if err == nil && len(encodedAll) <= maxChunkSize {
			chunks = append(chunks, providers.RenderChunk{
				WebPBytes:  encodedAll,
				DurationMs: candDurMs,
			})
			if totalSecs > 0 {
				remainingSecs = max(1, remainingSecs-candDurMs/1000)
			}
			start = maxEnd
			continue
		}

		low := start + 1
		high := maxEnd
		bestEnd := low
		var bestBytes []byte
		bestDurMs := chunkDurationFor(start, bestEnd)

		for low <= high {
			mid := (low + high) / 2
			chunkCandidate := frames[start:mid]
			midDurMs := chunkDurationFor(start, mid)
			encoded, err := encodeRange(chunkCandidate, midDurMs, holdMs)
			if err == nil && len(encoded) <= maxChunkSize {
				bestEnd = mid
				bestBytes = encoded
				bestDurMs = midDurMs
				low = mid + 1 // try to fit more frames
			} else {
				high = mid - 1 // too big or encode failed; try fewer frames
			}
		}

		if bestBytes == nil {
			// Even a single frame exceeded maxChunkSize!
			bestEnd = start + 1
			bestDurMs = chunkDurationFor(start, bestEnd)
			bestBytes, err = encodeRange(frames[start:bestEnd], bestDurMs, holdMs)
			if err != nil {
				return nil, fmt.Errorf("encoding single-frame fallback at frame %d: %w", start, err)
			}
			if len(bestBytes) > maxChunkSize {
				return nil, fmt.Errorf("single frame %d is %d bytes, exceeding maximum chunk budget of %d bytes", start, len(bestBytes), maxChunkSize)
			}
		}

		chunks = append(chunks, providers.RenderChunk{
			WebPBytes:  bestBytes,
			DurationMs: bestDurMs,
		})
		if totalSecs > 0 {
			remainingSecs = max(1, remainingSecs-bestDurMs/1000)
		}
		start = bestEnd
	}

	if len(chunks) > 0 {
		chunks[0].FullWebPBytes = allBytes
	}

	return chunks, nil
}
