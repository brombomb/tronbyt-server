package screeny

import (
	"image"
	"image/color"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateRandomFrames(count int, width, height int, seed int64) []*image.RGBA {
	r := rand.New(rand.NewSource(seed))
	frames := make([]*image.RGBA, count)
	for i := range count {
		img := image.NewRGBA(image.Rect(0, 0, width, height))
		for y := range height {
			for x := range width {
				img.SetRGBA(x, y, color.RGBA{
					R: byte(r.Intn(256)),
					G: byte(r.Intn(256)),
					B: byte(r.Intn(256)),
					A: 255,
				})
			}
		}
		frames[i] = img
	}
	return frames
}

func TestChunker_SingleChunk_HighCapacity(t *testing.T) {
	// 60 frames (~2 seconds at 30 fps)
	frames := generateRandomFrames(60, 64, 32, 42)
	maxChunkSize := 5 * 1024 * 1024 // 5 MB for Tronberry / Raspberry Pi

	chunks, err := SplitFramesIntoChunks(frames, 30, maxChunkSize)
	require.NoError(t, err)
	require.Len(t, chunks, 1, "should fit in a single chunk under 5MB")

	assert.True(t, len(chunks[0].WebPBytes) > 0)
	assert.True(t, len(chunks[0].WebPBytes) <= maxChunkSize)
	// 60 frames * (1000/30 = 33ms) = 1980ms
	assert.Equal(t, 60*33, chunks[0].DurationMs)
}

func TestChunker_MultiChunk_Constrained(t *testing.T) {
	// Generate 90 frames of random content so it has high entropy and doesn't compress too small
	frames := generateRandomFrames(90, 64, 32, 99)

	// First, check total unconstrained encoded size
	fullBytes, err := EncodeFramesToWebP(frames, 33*time.Millisecond)
	require.NoError(t, err)
	t.Logf("Full animation encoded size: %d bytes across %d frames", len(fullBytes), len(frames))

	// Constrain maxChunkSize to roughly 1/3 of total size to force multi-chunk splitting
	maxChunkSize := len(fullBytes) / 3
	require.Greater(t, maxChunkSize, 500, "chunk size budget should be reasonable")

	chunks, err := SplitFramesIntoChunks(frames, 30, maxChunkSize)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(chunks), 3, "should split into at least 3 chunks")

	var totalDurationMs int
	for i, chunk := range chunks {
		t.Logf("Chunk %d: size=%d bytes, duration=%d ms", i, len(chunk.WebPBytes), chunk.DurationMs)
		assert.LessOrEqual(t, len(chunk.WebPBytes), maxChunkSize, "chunk %d exceeded maxChunkSize", i)
		assert.Greater(t, chunk.DurationMs, 0, "chunk %d has non-positive duration", i)
		totalDurationMs += chunk.DurationMs
	}

	expectedTotalDuration := len(frames) * 33
	assert.Equal(t, expectedTotalDuration, totalDurationMs, "sum of chunk durations must equal full animation duration")
}

func TestChunker_ESP32_Constraint(t *testing.T) {
	// For ESP32, default limit is 460 KB (471,040 bytes)
	const esp32MaxBytes = 471040

	// 30 frames fits well within 460 KB
	frames := generateRandomFrames(30, 64, 32, 123)
	chunks, err := SplitFramesIntoChunks(frames, 30, esp32MaxBytes)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	assert.LessOrEqual(t, len(chunks[0].WebPBytes), esp32MaxBytes)
	assert.Equal(t, 30*33, chunks[0].DurationMs)
}
