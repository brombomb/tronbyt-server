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

// SplitFramesIntoChunks partitions sequential frames into animated WebP chunks,
// each fitting under maxChunkSize bytes with exact playback DurationMs.
func SplitFramesIntoChunks(frames []*image.RGBA, fps int, maxChunkSize int) ([]providers.RenderChunk, error) {
	if len(frames) == 0 {
		return nil, errors.New("no frames to split")
	}

	if fps <= 0 {
		fps = DefaultFPS
	}

	frameDurationMs := 1000 / fps
	frameDuration := time.Duration(frameDurationMs) * time.Millisecond

	// 1. If maxChunkSize <= 0, don't chunk: encode everything into a single chunk.
	if maxChunkSize <= 0 {
		b, err := EncodeFramesToWebP(frames, frameDuration)
		if err != nil {
			return nil, err
		}
		return []providers.RenderChunk{
			{
				WebPBytes:     b,
				DurationMs:    len(frames) * frameDurationMs,
				FullWebPBytes: b,
			},
		}, nil
	}

	// 2. Try encoding the entire animation in one chunk first.
	allBytes, err := EncodeFramesToWebP(frames, frameDuration)
	if err == nil && len(allBytes) <= maxChunkSize {
		return []providers.RenderChunk{
			{
				WebPBytes:     allBytes,
				DurationMs:    len(frames) * frameDurationMs,
				FullWebPBytes: allBytes,
			},
		}, nil
	}

	// 3. Multi-chunk splitting:
	// Binary-search the maximum number of frames that fit within maxChunkSize for each chunk.
	var chunks []providers.RenderChunk
	start := 0
	totalFrames := len(frames)

	for start < totalFrames {
		low := start + 1
		high := totalFrames
		bestEnd := low
		var bestBytes []byte

		for low <= high {
			mid := (low + high) / 2
			chunkCandidate := frames[start:mid]
			encoded, err := EncodeFramesToWebP(chunkCandidate, frameDuration)
			if err == nil && len(encoded) <= maxChunkSize {
				bestEnd = mid
				bestBytes = encoded
				low = mid + 1 // try to fit more frames
			} else {
				high = mid - 1 // too big or encode failed; try fewer frames
			}
		}

		if bestBytes == nil {
			// Even a single frame exceeded maxChunkSize! Emit at least 1 frame to guarantee progress.
			bestEnd = start + 1
			bestBytes, err = EncodeFramesToWebP(frames[start:bestEnd], frameDuration)
			if err != nil {
				return nil, fmt.Errorf("encoding single-frame fallback at frame %d: %w", start, err)
			}
		}

		chunkFrames := bestEnd - start
		chunks = append(chunks, providers.RenderChunk{
			WebPBytes:  bestBytes,
			DurationMs: chunkFrames * frameDurationMs,
		})
		start = bestEnd
	}

	if len(chunks) > 0 {
		chunks[0].FullWebPBytes = allBytes
	}

	return chunks, nil
}
