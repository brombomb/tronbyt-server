package screeny

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
	"strconv"
	"strings"
	"sync"
	"time"

	"tronbyt-server/internal/providers"

	"github.com/gorilla/websocket"
)

// AdapterConfig holds configuration settings for the ScreenyAdapter.
type AdapterConfig struct {
	StudioURL    string
	FPS          int
	MaxWebPBytes int
	ScaleFilter  string
	HTTPClient   *http.Client
	WSUpgrader   *websocket.Dialer
}

// ScreenyAdapter implements the providers.ExternalAppProvider interface for gotwalt/screeny.
type ScreenyAdapter struct {
	mu           sync.Mutex
	studioURL    string
	fps          int
	maxWebPBytes int
	scaleFilter  string
	httpClient   *http.Client
	wsDialer     *websocket.Dialer

	// Cached bootstrap data to avoid re-querying on every schema request.
	cacheMu     sync.RWMutex
	cachedApps  []providers.ExternalAppMeta
	cachedPatch map[string]StudioPatch
	lastCached  time.Time
}

// NewAdapter creates a new ScreenyAdapter with the provided configuration.
func NewAdapter(cfg AdapterConfig) *ScreenyAdapter {
	studioURL := strings.TrimRight(cfg.StudioURL, "/")
	if studioURL == "" {
		studioURL = "http://localhost:8787"
	}
	fps := cfg.FPS
	if fps <= 0 {
		fps = DefaultFPS
	}
	maxBytes := cfg.MaxWebPBytes
	if maxBytes <= 0 {
		maxBytes = 5242880 // 5 MB default
	}

	scaleFilter := cfg.ScaleFilter
	if scaleFilter == "" {
		scaleFilter = FilterBicubic
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	wsDialer := cfg.WSUpgrader
	if wsDialer == nil {
		wsDialer = &websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
		}
	}

	return &ScreenyAdapter{
		studioURL:    studioURL,
		fps:          fps,
		maxWebPBytes: maxBytes,
		scaleFilter:  scaleFilter,
		httpClient:   httpClient,
		wsDialer:     wsDialer,
		cachedPatch:  make(map[string]StudioPatch),
	}
}

// Name returns the provider's unique identifier.
func (a *ScreenyAdapter) Name() string {
	return "screeny"
}

// ListApps queries screeny-studio's /api/v1/bootstrap to discover available patches.
func (a *ScreenyAdapter) ListApps(ctx context.Context) ([]providers.ExternalAppMeta, error) {
	bootstrap, err := a.fetchBootstrap(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching screeny bootstrap: %w", err)
	}

	var metaList []providers.ExternalAppMeta
	patchMap := make(map[string]StudioPatch)

	for _, p := range bootstrap.Patches {
		patchMap[p.ID] = p
		// Skip patches explicitly marked unplayable on this host.
		if !p.Playable && p.UnplayableReason != nil {
			slog.Debug("Screeny patch marked unplayable, skipping", "id", p.ID, "reason", *p.UnplayableReason)
			continue
		}

		meta := providers.ExternalAppMeta{
			ID:         p.ID,
			Name:       p.Name,
			Summary:    p.Blurb,
			Desc:       p.Blurb,
			Author:     "Screeny",
			Provider:   a.Name(),
			Supports2x: true,
			Tags:       []string{"screeny", "30fps"},
		}
		metaList = append(metaList, meta)
	}

	a.cacheMu.Lock()
	a.cachedApps = metaList
	a.cachedPatch = patchMap
	a.lastCached = time.Now()
	a.cacheMu.Unlock()

	return metaList, nil
}

// GetAppSchema returns the Pixlet-compatible JSON schema for the specified Screeny patch ID.
func (a *ScreenyAdapter) GetAppSchema(ctx context.Context, appID string) ([]byte, error) {
	a.cacheMu.RLock()
	patch, found := a.cachedPatch[appID]
	a.cacheMu.RUnlock()

	if !found {
		// Cache miss; refresh bootstrap once
		if _, err := a.ListApps(ctx); err != nil {
			return nil, fmt.Errorf("loading screeny patches for schema %s: %w", appID, err)
		}
		a.cacheMu.RLock()
		patch, found = a.cachedPatch[appID]
		a.cacheMu.RUnlock()
	}

	if !found {
		return nil, fmt.Errorf("patch %q not found in screeny-studio", appID)
	}

	return TranslatePatchParamsToPixletSchema(patch.Params)
}

// RenderAppChunks renders a Screeny patch at 30 FPS for the requested duration,
// capturing frames and packaging them into WebP chunks.
func (a *ScreenyAdapter) RenderAppChunks(ctx context.Context, req providers.RenderRequest) ([]providers.RenderChunk, error) {
	// Mutex lock to ensure sequential rendering access to screeny-studio instance.
	a.mu.Lock()
	defer a.mu.Unlock()

	fps := req.FPS
	if fps <= 0 {
		fps = a.fps
	}
	if fps <= 0 {
		fps = DefaultFPS
	}

	durationMs := req.DurationMs
	if durationMs <= 0 {
		durationMs = 10000 // 10s default
	}

	maxChunkSize := req.MaxChunkSize
	if maxChunkSize <= 0 {
		maxChunkSize = a.maxWebPBytes
	}

	// 1. Activate the target patch in screeny-studio
	if err := a.activatePatch(ctx, req.AppID); err != nil {
		return nil, fmt.Errorf("activating patch %s: %w", req.AppID, err)
	}

	scaleFilter := a.scaleFilter
	if sf, ok := req.Config["scale_filter"].(string); ok && sf != "" && sf != "default" {
		scaleFilter = sf
	}

	// 2. Apply config parameters
	for paramID, val := range req.Config {
		if paramID == "scale_filter" {
			continue // Handled internally by server-side scaling pipeline
		}
		if fVal, ok := parseParamValue(val); ok {
			if err := a.setParam(ctx, paramID, fVal); err != nil {
				slog.Warn("Failed to set screeny parameter", "param", paramID, "value", fVal, "error", err)
			}
		}
	}

	// 3. Connect to screeny-studio WebSocket preview stream and capture frames
	frames, err := a.captureFrames(ctx, fps, durationMs, req.Width, req.Height, scaleFilter)
	if err != nil {
		return nil, fmt.Errorf("capturing screeny frames: %w", err)
	}

	if len(frames) == 0 {
		return nil, errors.New("captured zero frames from screeny-studio")
	}

	// 4. Split captured frames into animated WebP chunks
	return SplitFramesIntoChunks(frames, fps, maxChunkSize)
}

// fetchBootstrap calls GET /api/v1/bootstrap on screeny-studio.
func (a *ScreenyAdapter) fetchBootstrap(ctx context.Context) (*StudioBootstrap, error) {
	u := fmt.Sprintf("%s/api/v1/bootstrap", a.studioURL)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bootstrap returned status %d: %s", resp.StatusCode, string(body))
	}

	var bootstrap StudioBootstrap
	if err := json.NewDecoder(resp.Body).Decode(&bootstrap); err != nil {
		return nil, fmt.Errorf("decoding bootstrap JSON: %w", err)
	}

	return &bootstrap, nil
}

// activatePatch calls POST /api/v1/set_patch on screeny-studio.
func (a *ScreenyAdapter) activatePatch(ctx context.Context, patchID string) error {
	u := fmt.Sprintf("%s/api/v1/set_patch", a.studioURL)
	payload, err := json.Marshal(map[string]string{"id": patchID})
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set_patch status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// setParam calls POST /api/v1/set_param on screeny-studio.
func (a *ScreenyAdapter) setParam(ctx context.Context, paramID string, val float32) error {
	u := fmt.Sprintf("%s/api/v1/set_param", a.studioURL)
	payload, err := json.Marshal(map[string]any{
		"id":    paramID,
		"value": val,
	})
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set_param status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// captureFrames opens a WebSocket to /api/v1/ws and collects frames for durationMs.
func (a *ScreenyAdapter) captureFrames(ctx context.Context, fps, durationMs, targetWidth, targetHeight int, scaleFilter string) ([]*image.RGBA, error) {
	u, err := url.Parse(a.studioURL)
	if err != nil {
		return nil, fmt.Errorf("parsing studio URL: %w", err)
	}

	wsScheme := "ws"
	if u.Scheme == "https" {
		wsScheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s/api/v1/ws?fps=%d&repeat=true&overview=false", wsScheme, u.Host, fps)

	conn, _, err := a.wsDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dialing screeny ws %s: %w", wsURL, err)
	}
	defer func() { _ = conn.Close() }()

	targetFrameCount := (durationMs * fps) / 1000
	if targetFrameCount <= 0 {
		targetFrameCount = 30
	}

	scaleRequired := targetWidth == 128 && targetHeight == 64
	var frames []*image.RGBA
	timeout := time.After(time.Duration(durationMs+3000) * time.Millisecond) // safety buffer

	for len(frames) < targetFrameCount {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			// If we got some frames before timeout, return what we have
			if len(frames) > 0 {
				return frames, nil
			}
			return nil, errors.New("timeout waiting for screeny frames")
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		msgType, msgData, err := conn.ReadMessage()
		if err != nil {
			if len(frames) > 0 {
				break
			}
			return nil, fmt.Errorf("reading ws frame: %w", err)
		}

		if msgType != websocket.BinaryMessage {
			continue // skip JSON heartbeats / state messages
		}

		frame, err := DecodeScreenyFrame(msgData)
		if err != nil {
			slog.Debug("Failed to decode screeny frame, skipping", "error", err)
			continue
		}

		if scaleRequired {
			frame = ScaleFrame2x(frame, scaleFilter)
		}

		frames = append(frames, frame)
	}

	return frames, nil
}

// parseParamValue parses user-provided form configuration into a float32 expected by Screeny.
func parseParamValue(val any) (float32, bool) {
	switch v := val.(type) {
	case float64:
		return float32(v), true
	case float32:
		return v, true
	case int:
		return float32(v), true
	case int64:
		return float32(v), true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	case string:
		if strings.EqualFold(v, "true") {
			return 1.0, true
		}
		if strings.EqualFold(v, "false") {
			return 0.0, true
		}
		if parsed, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(parsed), true
		}
	}
	return 0, false
}
