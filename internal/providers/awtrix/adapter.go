package awtrix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tronbyt-server/internal/providers"
	"tronbyt-server/internal/providers/screeny"
)

// DefaultFPS is 15 frames per second for Awtrix capture.
const DefaultFPS = 15

// AdapterConfig holds parameters to configure the AwtrixAdapter.
type AdapterConfig struct {
	BaseURL     string
	FPS         int
	ScaleFilter string
	Layout      string
	HTTPClient  *http.Client
}

// AwtrixAdapter implements providers.ExternalAppProvider for AWTRIX NG.
type AwtrixAdapter struct {
	sem         chan struct{}
	baseURL     string
	fps         int
	scaleFilter string
	layout      string
	httpClient  *http.Client
}

// NewAdapter creates a new AwtrixAdapter.
func NewAdapter(cfg AdapterConfig) *AwtrixAdapter {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "http://localhost:7080"
	}
	fps := cfg.FPS
	if fps <= 0 {
		fps = DefaultFPS
	}
	scaleFilter := cfg.ScaleFilter
	if scaleFilter == "" {
		scaleFilter = screeny.FilterBicubic
	}
	layout := cfg.Layout
	if layout == "" {
		layout = "center"
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	return &AwtrixAdapter{
		sem:         make(chan struct{}, 1),
		baseURL:     baseURL,
		fps:         fps,
		scaleFilter: scaleFilter,
		layout:      layout,
		httpClient:  httpClient,
	}
}

// Name returns the provider's unique identifier.
func (a *AwtrixAdapter) Name() string {
	return "awtrix"
}

type awtrixAppItem struct {
	Name    string         `json:"name"`
	Enabled bool           `json:"enabled"`
	InLoop  bool           `json:"inLoop"`
	Present bool           `json:"present"`
	Origin  string         `json:"origin"`
	Config  bool           `json:"config"`
	Meta    *awtrixAppMeta `json:"meta,omitempty"`
}

type awtrixAppMeta struct {
	Name    string `json:"name,omitempty"`
	Desc    string `json:"desc,omitempty"`
	Author  string `json:"author,omitempty"`
	Version string `json:"version,omitempty"`
}

// ListApps queries AWTRIX NG's /api/v1/apps endpoint to discover installed and built-in apps.
func (a *AwtrixAdapter) ListApps(ctx context.Context) ([]providers.ExternalAppMeta, error) {
	u := fmt.Sprintf("%s/api/v1/apps", a.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("creating apps request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying awtrix apps: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("awtrix apps returned status %d: %s", resp.StatusCode, string(body))
	}

	var items []awtrixAppItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decoding awtrix apps response: %w", err)
	}

	var result []providers.ExternalAppMeta
	for _, item := range items {
		if !item.Present && item.Origin != "builtin" {
			continue // skip uninstalled scripts
		}

		name := item.Name
		summary := fmt.Sprintf("AWTRIX NG %s app", item.Name)
		desc := summary
		author := "AWTRIX NG"

		if item.Meta != nil {
			if item.Meta.Name != "" {
				name = item.Meta.Name
			}
			if item.Meta.Desc != "" {
				summary = item.Meta.Desc
				desc = item.Meta.Desc
			}
			if item.Meta.Author != "" {
				author = item.Meta.Author
			}
		}

		result = append(result, providers.ExternalAppMeta{
			ID:      item.Name,
			Name:    name,
			Summary: summary,
			Desc:    desc,
			Author:  author,
		})
	}

	return result, nil
}

// GetAppSchema returns the Pixlet-compatible configuration schema for an Awtrix app.
func (a *AwtrixAdapter) GetAppSchema(ctx context.Context, appID string) ([]byte, error) {
	cfg, isBuiltin, err := a.fetchAppConfig(ctx, appID)
	if err != nil {
		slog.Warn("Failed to fetch awtrix app config, returning empty schema", "app", appID, "error", err)
		return []byte(`{"version":"1","schema":[]}`), nil
	}

	return TranslateAwtrixSchema(*cfg, isBuiltin)
}

func (a *AwtrixAdapter) fetchAppConfig(ctx context.Context, appID string) (*AwtrixConfigResponse, bool, error) {
	// Try builtin URL first
	builtinURL := fmt.Sprintf("%s/api/v1/apps/builtin/%s/config", a.baseURL, url.PathEscape(appID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, builtinURL, nil)
	if err != nil {
		return nil, false, err
	}

	resp, err := a.httpClient.Do(req)
	if err == nil && resp.StatusCode == http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		var cfg AwtrixConfigResponse
		if err := json.NewDecoder(resp.Body).Decode(&cfg); err == nil {
			return &cfg, true, nil
		}
	}
	if resp != nil {
		_ = resp.Body.Close()
	}

	// Try non-builtin script URL
	scriptURL := fmt.Sprintf("%s/api/v1/apps/%s/config", a.baseURL, url.PathEscape(appID))
	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, scriptURL, nil)
	if err != nil {
		return nil, false, err
	}

	resp2, err := a.httpClient.Do(req2)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("fetching config returned status %d", resp2.StatusCode)
	}

	var cfg AwtrixConfigResponse
	if err := json.NewDecoder(resp2.Body).Decode(&cfg); err != nil {
		return nil, false, err
	}

	return &cfg, false, nil
}

type screenData struct {
	Width  int   `json:"width"`
	Height int   `json:"height"`
	Pixels []int `json:"pixels"`
}

// RenderAppChunks configures the app on AWTRIX NG, captures virtual screen frames, and encodes them into WebP chunks.
func (a *AwtrixAdapter) RenderAppChunks(ctx context.Context, req providers.RenderRequest) ([]providers.RenderChunk, error) {
	// Acquire semaphore to ensure exclusive access to the Awtrix virtual display
	select {
	case a.sem <- struct{}{}:
		defer func() { <-a.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// 1. If custom configuration options were provided, apply them via PATCH
	if len(req.Config) > 0 {
		if cfg, isBuiltin, err := a.fetchAppConfig(ctx, req.AppID); err == nil && len(cfg.Fields) > 0 {
			patchBody := ConvertUserConfigToAwtrix(cfg.Fields, req.Config)
			if len(patchBody) > 0 {
				patchURL := fmt.Sprintf("%s/api/v1/apps/builtin/%s/config", a.baseURL, url.PathEscape(req.AppID))
				if !isBuiltin {
					patchURL = fmt.Sprintf("%s/api/v1/apps/%s/config", a.baseURL, url.PathEscape(req.AppID))
				}
				bodyBytes, err := json.Marshal(patchBody)
				if err == nil {
					pReq, err := http.NewRequestWithContext(ctx, http.MethodPatch, patchURL, bytes.NewReader(bodyBytes))
					if err == nil {
						pReq.Header.Set("Content-Type", "application/json")
						if pResp, err := a.httpClient.Do(pReq); err == nil {
							_ = pResp.Body.Close()
						}
					}
				}
			}
		}
	}

	// 2. Freeze AWTRIX auto-rotation and disable transitions so only this single app renders
	freezeURL := fmt.Sprintf("%s/api/v1/settings", a.baseURL)
	freezeBody := []byte(`{"autoTransition":false,"transitionDurationMs":0}`)
	if fReq, err := http.NewRequestWithContext(ctx, http.MethodPatch, freezeURL, bytes.NewReader(freezeBody)); err == nil {
		fReq.Header.Set("Content-Type", "application/json")
		if fResp, err := a.httpClient.Do(fReq); err == nil {
			_ = fResp.Body.Close()
		}
	}

	// 3. Activate the app on the AWTRIX display
	activateURL := fmt.Sprintf("%s/api/v1/apps/active", a.baseURL)
	activateBody, _ := json.Marshal(map[string]string{"name": req.AppID})
	actReq, err := http.NewRequestWithContext(ctx, http.MethodPut, activateURL, bytes.NewReader(activateBody))
	if err != nil {
		return nil, fmt.Errorf("creating activate request: %w", err)
	}
	actReq.Header.Set("Content-Type", "application/json")

	actResp, err := a.httpClient.Do(actReq)
	if err != nil {
		return nil, fmt.Errorf("activating awtrix app: %w", err)
	}
	_ = actResp.Body.Close()

	// Wait 150ms for initial layout rendering
	select {
	case <-time.After(150 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// 4. Determine capture parameters
	fps := req.FPS
	if fps <= 0 {
		fps = a.fps
	}
	if fps > 30 {
		fps = 30
	}
	durationMs := req.DurationMs
	if durationMs <= 0 {
		durationMs = 10000
	}

	targetFrames := max(1, (durationMs*fps)/1000)

	frameInterval := time.Duration(1000/fps) * time.Millisecond
	screenURL := fmt.Sprintf("%s/api/v1/display/screen", a.baseURL)

	scaleRequired := req.Width == 128 && req.Height == 64
	var frames []*image.RGBA
	var layoutDetermined bool
	var layoutMode string

	// 5. Capture frames
	ticker := time.NewTicker(frameInterval)
	defer ticker.Stop()

	for range targetFrames {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}

		sReq, err := http.NewRequestWithContext(ctx, http.MethodGet, screenURL, nil)
		if err != nil {
			continue
		}
		sResp, err := a.httpClient.Do(sReq)
		if err != nil {
			continue
		}

		var sd screenData
		decodeErr := json.NewDecoder(sResp.Body).Decode(&sd)
		_ = sResp.Body.Close()

		if decodeErr != nil || sd.Width <= 0 || sd.Height <= 0 || len(sd.Pixels) != sd.Width*sd.Height {
			continue
		}

		// Convert pixel array to image.RGBA
		img := image.NewRGBA(image.Rect(0, 0, sd.Width, sd.Height))
		for idx, pix := range sd.Pixels {
			x := idx % sd.Width
			y := idx / sd.Width
			offset := img.PixOffset(x, y)
			img.Pix[offset] = uint8((pix >> 16) & 0xFF)
			img.Pix[offset+1] = uint8((pix >> 8) & 0xFF)
			img.Pix[offset+2] = uint8(pix & 0xFF)
			img.Pix[offset+3] = 0xFF
		}

		// Determine layout mode on the first frame with visible pixels
		if !layoutDetermined && a.layout != "native" && sd.Width == 64 && sd.Height == 32 {
			maxY := 0
			maxX := 0
			hasPixels := false
			for y := range 32 {
				for x := range 64 {
					off := img.PixOffset(x, y)
					if img.Pix[off] != 0 || img.Pix[off+1] != 0 || img.Pix[off+2] != 0 {
						hasPixels = true
						if y > maxY {
							maxY = y
						}
						if x > maxX {
							maxX = x
						}
					}
				}
			}
			if hasPixels {
				layoutDetermined = true
				if maxY <= 7 {
					layoutMode = "8row"
				} else if maxY <= 15 {
					if maxX <= 52 {
						layoutMode = "16row-narrow"
					} else {
						layoutMode = "16row-full"
					}
				} else {
					layoutMode = "32row"
				}
			}
		}

		if a.layout != "native" && layoutDetermined && sd.Width == 64 && sd.Height == 32 {
			img = applyAwtrixLayout(img, layoutMode)
		}

		if scaleRequired {
			img = screeny.ScaleFrame2x(img, a.scaleFilter)
		}

		frames = append(frames, img)
	}

	if len(frames) == 0 {
		return nil, errors.New("no frames captured from awtrix screen")
	}

	// 5. Partition frames into WebP chunk(s)
	return screeny.SplitFramesIntoChunks(frames, fps, req.MaxChunkSize, req.MaxChunkDurationMs)
}

func applyAwtrixLayout(src *image.RGBA, mode string) *image.RGBA {
	switch mode {
	case "8row":
		// 8-row app (e.g. Ghost Parade: 32x8): 2x integer scale to 64x16, centered vertically at Y=8..23
		dst := image.NewRGBA(image.Rect(0, 0, 64, 32))
		for y := 0; y <= 7; y++ {
			for x := 0; x <= 31; x++ {
				srcOff := src.PixOffset(x, y)
				r, g, b, a := src.Pix[srcOff], src.Pix[srcOff+1], src.Pix[srcOff+2], src.Pix[srcOff+3]
				for dy := range 2 {
					for dx := range 2 {
						dstOff := dst.PixOffset(x*2+dx, 8+y*2+dy)
						dst.Pix[dstOff] = r
						dst.Pix[dstOff+1] = g
						dst.Pix[dstOff+2] = b
						dst.Pix[dstOff+3] = a
					}
				}
			}
		}
		return dst

	case "16row-narrow":
		// 16-row 52-column app (e.g. Beautiful Weather: 52x16): center horizontally at X=6, vertically at Y=8
		dst := image.NewRGBA(image.Rect(0, 0, 64, 32))
		for y := 0; y <= 15; y++ {
			for x := 0; x <= 51; x++ {
				if x+6 >= 64 {
					continue
				}
				srcOff := src.PixOffset(x, y)
				dstOff := dst.PixOffset(x+6, y+8)
				copy(dst.Pix[dstOff:dstOff+4], src.Pix[srcOff:srcOff+4])
			}
		}
		return dst

	case "16row-full":
		// 16-row 64-column app (e.g. Pixel Aquarium: 64x16): center vertically at Y=8 across full width
		dst := image.NewRGBA(image.Rect(0, 0, 64, 32))
		for y := 0; y <= 15; y++ {
			for x := 0; x <= 63; x++ {
				srcOff := src.PixOffset(x, y)
				dstOff := dst.PixOffset(x, y+8)
				copy(dst.Pix[dstOff:dstOff+4], src.Pix[srcOff:srcOff+4])
			}
		}
		return dst

	default:
		return src
	}
}
