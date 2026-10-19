package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tronbyt-server/internal/data"
	"tronbyt-server/internal/providers"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Dummy 1x1 green webp bytes for testing.
var dummyChunkWebP1 = []byte{
	0x52, 0x49, 0x46, 0x46, 0x24, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38, 0x20,
	0x18, 0x00, 0x00, 0x00, 0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0x3e,
	0x0d, 0x03, 0x00, 0xfe, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01,
}

var dummyChunkWebP2 = []byte{
	0x52, 0x49, 0x46, 0x46, 0x24, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38, 0x20,
	0x18, 0x00, 0x00, 0x00, 0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0x3e,
	0x0d, 0x03, 0x00, 0xfe, 0xff, 0xff, 0x00, 0x00, 0x00, 0x02,
}

var dummyChunkWebP3 = []byte{
	0x52, 0x49, 0x46, 0x46, 0x24, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38, 0x20,
	0x18, 0x00, 0x00, 0x00, 0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0x3e,
	0x0d, 0x03, 0x00, 0xfe, 0xff, 0xff, 0x00, 0x00, 0x00, 0x03,
}

type mockExternalProvider struct {
	name   string
	apps   []providers.ExternalAppMeta
	chunks []providers.RenderChunk
}

func (m *mockExternalProvider) Name() string { return m.name }
func (m *mockExternalProvider) ListApps(ctx context.Context) ([]providers.ExternalAppMeta, error) {
	return m.apps, nil
}
func (m *mockExternalProvider) GetAppSchema(ctx context.Context, appID string) ([]byte, error) {
	return []byte("{}"), nil
}
func (m *mockExternalProvider) RenderAppChunks(ctx context.Context, req providers.RenderRequest) ([]providers.RenderChunk, error) {
	return m.chunks, nil
}

func TestDeviceMaxChunkSize_ESP_vs_RPi(t *testing.T) {
	s := newTestServerAPI(t)
	s.Config.ScreenyMaxWebPBytesESP = 471040
	s.Config.ScreenyMaxWebPBytesRPi = 5242880

	espDevice := &data.Device{
		ID:   "esp1",
		Type: data.DeviceTidbytGen1,
	}
	assert.Equal(t, 471040, s.GetMaxChunkSizeForDevice(espDevice))

	matrixDevice := &data.Device{
		ID:   "matrix1",
		Type: data.DeviceMatrixPortal,
	}
	assert.Equal(t, 471040, s.GetMaxChunkSizeForDevice(matrixDevice))

	rpiDevice := &data.Device{
		ID:   "rpi1",
		Type: data.DeviceRaspberryPi,
	}
	assert.Equal(t, 5242880, s.GetMaxChunkSizeForDevice(rpiDevice))

	tronberryDevice := &data.Device{
		ID:   "tronberry1",
		Type: data.DeviceTronbytS3,
		Info: data.DeviceInfo{
			FirmwareType: "Tronberry",
		},
	}
	assert.Equal(t, 5242880, s.GetMaxChunkSizeForDevice(tronberryDevice))
}

func TestMultiChunk_HTTP_SequentialServing(t *testing.T) {
	s := newTestServerAPI(t)
	ctx := context.Background()

	user := data.User{Username: "chunkuser"}
	require.NoError(t, gorm.G[data.User](s.DB).Create(ctx, &user))

	device := data.Device{
		ID:              "chunk_dev_1",
		Username:        user.Username,
		Name:            "MultiChunk Dev",
		APIKey:          "key123",
		Brightness:      80,
		DefaultInterval: 10,
		LastAppIndex:    -1,
	}
	require.NoError(t, gorm.G[data.Device](s.DB).Create(ctx, &device))

	// App 1: Multi-chunk app (3 chunks)
	path1 := "screeny:clock"
	app1 := data.App{
		DeviceID: device.ID,
		Iname:    "app1",
		Name:     "Clock",
		Enabled:  true,
		Order:    0,
		Path:     &path1,
		Pushed:   false,
	}
	require.NoError(t, gorm.G[data.App](s.DB).Create(ctx, &app1))

	// App 2: Standard single-chunk app
	path2 := "pushed:2"
	app2 := data.App{
		DeviceID:    device.ID,
		Iname:       "app2",
		Name:        "Weather",
		Enabled:     true,
		Order:       1,
		Path:        &path2,
		Pushed:      true,
		DisplayTime: 7,
	}
	require.NoError(t, gorm.G[data.App](s.DB).Create(ctx, &app2))

	// Write App 2 dummy webp
	webpDir, err := s.ensureDeviceImageDir(device.ID)
	require.NoError(t, err)
	app2Path := s.getAppWebpPath(webpDir, &app2)
	require.NoError(t, os.MkdirAll(filepath.Dir(app2Path), 0755))
	require.NoError(t, os.WriteFile(app2Path, dummyChunkWebP3, 0644))

	// Save 3 chunks for App 1
	chunks := []providers.RenderChunk{
		{WebPBytes: dummyChunkWebP1, DurationMs: 3300},
		{WebPBytes: dummyChunkWebP2, DurationMs: 3300},
		{WebPBytes: dummyChunkWebP3, DurationMs: 3400},
	}
	s.RegisterExternalProvider(&mockExternalProvider{name: "screeny", chunks: chunks})
	require.NoError(t, s.saveAppChunks(device.ID, &app1, chunks))

	ts := httptest.NewServer(s)
	defer ts.Close()

	client := ts.Client()

	reqNext := func() (int, []byte, string) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/"+device.ID+"/next", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer key123")
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		dwell := resp.Header.Get("Tronbyt-Dwell-Secs")
		body := make([]byte, 1024)
		n, _ := resp.Body.Read(body)
		return resp.StatusCode, body[:n], dwell
	}

	// Call 1 -> Expect chunk 0 of App 1 (dwell 3s)
	code, body, dwell := reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP1, body)
	assert.Equal(t, "3", dwell)

	// Call 2 -> Expect chunk 1 of App 1 (dwell 3s)
	code, body, dwell = reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP2, body)
	assert.Equal(t, "3", dwell)

	// Call 3 -> Expect chunk 2 of App 1 (dwell 3s)
	code, body, dwell = reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP3, body)
	assert.Equal(t, "3", dwell)

	// Call 4 -> All chunks of App 1 done! Rotates to App 2 (dwell 7s)
	code, body, dwell = reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP3, body)
	assert.Equal(t, "7", dwell)

	// Call 5 -> Rotates back to App 1, chunk 0!
	code, body, dwell = reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP1, body)
	assert.Equal(t, "3", dwell)
}

func TestMultiChunk_HTTP_EphemeralInterrupt(t *testing.T) {
	s := newTestServerAPI(t)
	ctx := context.Background()

	user := data.User{Username: "ephuser"}
	require.NoError(t, gorm.G[data.User](s.DB).Create(ctx, &user))

	device := data.Device{
		ID:              "chunk_dev_eph",
		Username:        user.Username,
		Name:            "Eph Dev",
		APIKey:          "keyeph",
		Brightness:      80,
		DefaultInterval: 10,
	}
	require.NoError(t, gorm.G[data.Device](s.DB).Create(ctx, &device))

	path1 := "screeny:clock"
	app1 := data.App{
		DeviceID: device.ID,
		Iname:    "app1",
		Name:     "Clock",
		Enabled:  true,
		Order:    0,
		Path:     &path1,
		Pushed:   false,
	}
	require.NoError(t, gorm.G[data.App](s.DB).Create(ctx, &app1))

	chunks := []providers.RenderChunk{
		{WebPBytes: dummyChunkWebP1, DurationMs: 3300},
		{WebPBytes: dummyChunkWebP2, DurationMs: 3300},
	}
	s.RegisterExternalProvider(&mockExternalProvider{name: "screeny", chunks: chunks})
	require.NoError(t, s.saveAppChunks(device.ID, &app1, chunks))

	ts := httptest.NewServer(s)
	defer ts.Close()

	client := ts.Client()
	reqNext := func() (int, []byte) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/"+device.ID+"/next", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer keyeph")
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		body := make([]byte, 1024)
		n, _ := resp.Body.Read(body)
		return resp.StatusCode, body[:n]
	}

	// Request 1: serves chunk 0
	code, body := reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP1, body)

	// Now push an ephemeral image
	pushedDir := filepath.Join(s.DataDir, "webp", device.ID, "pushed")
	require.NoError(t, os.MkdirAll(pushedDir, 0755))
	ephemeralPath := filepath.Join(pushedDir, "__ephemeral_test.webp")
	require.NoError(t, os.WriteFile(ephemeralPath, dummyChunkWebP3, 0644))

	// Request 2: should immediately serve ephemeral image and interrupt chunk sequence
	code, body = reqNext()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, dummyChunkWebP3, body)
}

func TestMultiChunk_WebSocket_Streaming(t *testing.T) {
	s := newTestServerAPI(t)
	ctx := context.Background()

	user := data.User{Username: "wsuser"}
	require.NoError(t, gorm.G[data.User](s.DB).Create(ctx, &user))

	deviceID := "ws_chunk_dev"
	device := data.Device{
		ID:              deviceID,
		Username:        user.Username,
		Name:            "WS Chunk Dev",
		APIKey:          "wskey",
		Brightness:      50,
		DefaultInterval: 10,
	}
	require.NoError(t, gorm.G[data.Device](s.DB).Create(ctx, &device))

	path1 := "screeny:anim"
	app1 := data.App{
		DeviceID: deviceID,
		Iname:    "anim1",
		Name:     "Screeny Anim",
		Enabled:  true,
		Order:    0,
		Path:     &path1,
		Pushed:   false,
	}
	require.NoError(t, gorm.G[data.App](s.DB).Create(ctx, &app1))

	// 2 chunks with short durations for testing (100ms each)
	chunks := []providers.RenderChunk{
		{WebPBytes: dummyChunkWebP1, DurationMs: 100},
		{WebPBytes: dummyChunkWebP2, DurationMs: 100},
	}
	s.RegisterExternalProvider(&mockExternalProvider{name: "screeny", chunks: chunks})
	require.NoError(t, s.saveAppChunks(deviceID, &app1, chunks))

	ts := httptest.NewServer(s)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/" + deviceID + "/ws"
	dialer := websocket.Dialer{}
	conn, resp, err := dialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	assert.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	// Helper to read messages until a binary image is received
	readUntilImage := func() []byte {
		for {
			msgType, data, err := conn.ReadMessage()
			require.NoError(t, err)
			if msgType == websocket.BinaryMessage {
				return data
			}
			if msgType == websocket.TextMessage {
				var m map[string]any
				_ = json.Unmarshal(data, &m)
			}
		}
	}

	// 1. Should receive chunk 0 first
	img0 := readUntilImage()
	assert.Equal(t, dummyChunkWebP1, img0)

	// Send an ack from client
	_ = conn.WriteJSON(WSMessage{Displaying: new(int)})

	// 2. Should receive chunk 1 next after ~100ms
	start := time.Now()
	img1 := readUntilImage()
	elapsed := time.Since(start)
	assert.Equal(t, dummyChunkWebP2, img1)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(70), "Should wait approximately chunk DurationMs before sending next chunk")
}
