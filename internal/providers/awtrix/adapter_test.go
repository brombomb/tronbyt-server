package awtrix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"tronbyt-server/internal/providers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAwtrixAdapter_ListApps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/apps", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]awtrixAppItem{
			{
				Name:    "Time",
				Enabled: true,
				InLoop:  true,
				Present: true,
				Origin:  "builtin",
				Config:  true,
			},
			{
				Name:    "Date",
				Enabled: true,
				InLoop:  true,
				Present: true,
				Origin:  "builtin",
				Config:  true,
			},
		})
	}))
	defer server.Close()

	adapter := NewAdapter(AdapterConfig{
		BaseURL: server.URL,
	})

	assert.Equal(t, "awtrix", adapter.Name())

	apps, err := adapter.ListApps(context.Background())
	require.NoError(t, err)
	require.Len(t, apps, 2)
	assert.Equal(t, "Time", apps[0].ID)
	assert.Equal(t, "Time", apps[0].Name)
	assert.Equal(t, "Date", apps[1].ID)
}

func TestAwtrixAdapter_GetAppSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/apps/builtin/Time/config", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(AwtrixConfigResponse{
			Name: "Time",
			Fields: []AwtrixField{
				{
					Key:     "timeMode",
					Type:    "select",
					Options: []any{0, 1, 2},
					Default: 1,
					Value:   1,
				},
				{
					Key:     "time24h",
					Type:    "bool",
					Default: true,
					Value:   true,
				},
				{
					Key:     "timeColor",
					Type:    "color",
					Default: nil,
				},
			},
		})
	}))
	defer server.Close()

	adapter := NewAdapter(AdapterConfig{
		BaseURL: server.URL,
	})

	schemaBytes, err := adapter.GetAppSchema(context.Background(), "Time")
	require.NoError(t, err)

	var schema PixletSchema
	require.NoError(t, json.Unmarshal(schemaBytes, &schema))
	assert.Equal(t, "1", schema.Version)
	require.Len(t, schema.Schema, 3)

	assert.Equal(t, "timeMode", schema.Schema[0].ID)
	assert.Equal(t, "Clock layout", schema.Schema[0].Name)
	assert.Equal(t, "dropdown", schema.Schema[0].Type)
	assert.Equal(t, "time24h", schema.Schema[1].ID)
	assert.Equal(t, "onoff", schema.Schema[1].Type)
	assert.Equal(t, "timeColor", schema.Schema[2].ID)
	assert.Equal(t, "color", schema.Schema[2].Type)
}

func TestAwtrixAdapter_RenderAppChunks(t *testing.T) {
	activated := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/apps/active":
			assert.Equal(t, http.MethodPut, r.Method)
			activated = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/api/v1/display/screen":
			assert.Equal(t, http.MethodGet, r.Method)
			w.Header().Set("Content-Type", "application/json")
			// Return a 64x32 dummy screen
			pixels := make([]int, 64*32)
			for i := range pixels {
				pixels[i] = 0x00FF00 // green
			}
			_ = json.NewEncoder(w).Encode(screenData{
				Width:  64,
				Height: 32,
				Pixels: pixels,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter := NewAdapter(AdapterConfig{
		BaseURL: server.URL,
		FPS:     10,
	})

	req := providers.RenderRequest{
		AppID:        "Time",
		Width:        64,
		Height:       32,
		FPS:          10,
		DurationMs:   300, // 3 frames
		MaxChunkSize: 524288,
	}

	chunks, err := adapter.RenderAppChunks(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, activated, "App should have been activated via PUT")
	require.NotEmpty(t, chunks)
	assert.NotEmpty(t, chunks[0].WebPBytes)
}

func TestConvertUserConfigToAwtrix(t *testing.T) {
	fields := []AwtrixField{
		{
			Key:  "time24h",
			Type: "bool",
		},
		{
			Key:  "timeColor",
			Type: "color",
		},
		{
			Key:     "timeMode",
			Type:    "select",
			Options: []any{float64(0), float64(1)},
		},
		{
			Key:  "weekdayBar.show",
			Path: []string{"weekdayBar", "show"},
			Type: "bool",
		},
	}

	userConfig := map[string]any{
		"time24h":         "true",
		"timeColor":       "#ff0000",
		"timeMode":        "1",
		"weekdayBar.show": true,
	}

	converted := ConvertUserConfigToAwtrix(fields, userConfig)

	assert.Equal(t, true, converted["time24h"])
	assert.Equal(t, 16711680, converted["timeColor"])
	assert.Equal(t, 1, converted["timeMode"])

	wb, ok := converted["weekdayBar"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, wb["show"])
}
