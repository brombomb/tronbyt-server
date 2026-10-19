package screeny

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tronbyt-server/internal/providers"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScreenyAdapter_Lifecycle(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var setPatchReceived string
	setParamReceived := make(map[string]float32)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/bootstrap":
			bootstrap := StudioBootstrap{
				Patches: []StudioPatch{
					{
						ID:       "split_flap",
						Name:     "Split-Flap",
						Blurb:    "Airport departure board",
						Playable: true,
						Params: []StudioParam{
							{
								ID:      "style",
								Label:   "Board Style",
								Choices: []string{"Classic", "Modern"},
							},
						},
					},
					{
						ID:       "gpu_patch",
						Name:     "GPU Patch",
						Playable: false,
						UnplayableReason: func() *string {
							s := "requires GPU"
							return &s
						}(),
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(bootstrap)

		case "/api/v1/set_patch":
			var req map[string]string
			_ = json.NewDecoder(r.Body).Decode(&req)
			setPatchReceived = req["id"]
			w.WriteHeader(http.StatusOK)

		case "/api/v1/set_param":
			var req struct {
				ID    string  `json:"id"`
				Value float32 `json:"value"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			setParamReceived[req.ID] = req.Value
			w.WriteHeader(http.StatusOK)

		case "/api/v1/ws":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			// Send 10 preview frames
			previewPacket := make([]byte, PreviewPacketLen)
			for p := range NPixels {
				previewPacket[PreviewHeaderLen+p*3] = 100
				previewPacket[PreviewHeaderLen+p*3+1] = 150
				previewPacket[PreviewHeaderLen+p*3+2] = 200
			}

			for range 15 {
				if err := conn.WriteMessage(websocket.BinaryMessage, previewPacket); err != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter := NewAdapter(AdapterConfig{
		StudioURL:    server.URL,
		FPS:          30,
		MaxWebPBytes: 5242880,
	})

	ctx := context.Background()

	// 1. ListApps
	apps, err := adapter.ListApps(ctx)
	require.NoError(t, err)
	require.Len(t, apps, 1, "unplayable patch should be filtered out")
	assert.Equal(t, "split_flap", apps[0].ID)
	assert.Equal(t, "Split-Flap", apps[0].Name)
	assert.Equal(t, "screeny", apps[0].Provider)

	// 2. GetAppSchema
	schemaBytes, err := adapter.GetAppSchema(ctx, "split_flap")
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(schemaBytes), "Board Style"))

	// 3. RenderAppChunks (128x64 with scale_filter parameter)
	req := providers.RenderRequest{
		AppID:        "split_flap",
		Width:        128,
		Height:       64,
		FPS:          30,
		DurationMs:   300, // 300ms = 9 frames at 30 fps
		MaxChunkSize: 5242880,
		Config: map[string]any{
			"style":        "1",
			"scale_filter": "scale2x",
		},
	}

	chunks, err := adapter.RenderAppChunks(ctx, req)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)

	assert.Equal(t, "split_flap", setPatchReceived)
	assert.Equal(t, float32(1.0), setParamReceived["style"])
	assert.NotContains(t, setParamReceived, "scale_filter", "scale_filter should not be sent to screeny set_param")
	assert.True(t, len(chunks[0].WebPBytes) > 0)
}
