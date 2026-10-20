package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tronbyt-server/internal/apps"
	"tronbyt-server/internal/config"
	"tronbyt-server/internal/data"
	"tronbyt-server/internal/providers"
	"tronbyt-server/internal/providers/awtrix"
	"tronbyt-server/internal/providers/screeny"
	syncer "tronbyt-server/internal/sync"
	"tronbyt-server/web"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/gorilla/sessions"
	"github.com/gorilla/websocket"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"
	"golang.org/x/text/language"
	"gorm.io/gorm"
)

type Server struct {
	installCounts     map[string]int64 // public app ID -> Niblet install count
	usageRunning      atomic.Bool      // a community app stats sync is in progress
	usageForce        atomic.Bool      // a consent change awaits a sync that ignores the retry wait
	usageSyncs        sync.WaitGroup   // background syncs started by a consent change
	backgroundRenders sync.WaitGroup   // background renders triggered asynchronously
	DB                *gorm.DB
	Router            *http.ServeMux
	DataDir           string
	BaseTemplates     *template.Template
	PageTemplates     map[string]*template.Template
	Config            *config.Settings
	Store             *sessions.CookieStore
	Bundle            *i18n.Bundle // Add i18n bundle
	Broadcaster       *syncer.Broadcaster
	Upgrader          *websocket.Upgrader
	PromRegistry      prometheus.Registerer
	PromGatherer      prometheus.Gatherer
	metrics           *appMetrics
	OIDCProvider      *OIDCProvider

	systemAppsCache                      []apps.AppMetadata
	systemAppsCacheMutex                 sync.RWMutex
	systemAppsRefreshMutex               sync.Mutex
	systemAppsAutoRefreshPreferenceMutex sync.Mutex
	systemAppsAutoRefresh                atomic.Bool

	externalProviders     map[string]providers.ExternalAppProvider
	externalProvidersMu   sync.RWMutex
	appChunksCache        map[string][]providers.RenderChunk
	appChunksMu           sync.RWMutex
	deviceChunkIndices    map[string]int
	deviceCurrentChunkApp map[string]string
	deviceChunkMu         sync.Mutex
	inFlightRenders       map[string]bool
	renderPending         map[string]bool
	inFlightRendersMu     sync.Mutex
	thumbnailSingleFlight singleflight.Group

	// SchemaCache, when set, allows forcing a one-shot refetch of an app's
	// cached HTTP responses so dynamic schema data (e.g. dropdown options
	// fetched in get_schema) can be refreshed before the app's TTL expires.
	SchemaCache SchemaCacheBypasser

	UpdateAvailable  bool
	LatestReleaseURL string

	// githubAPIBase, when set, replaces https://api.github.com (used by tests).
	githubAPIBase string
}

// SchemaCacheBypasser forces cache reads for keys with the given prefix to miss
// for the duration of fn, so http.get calls made during fn refetch from origin.
type SchemaCacheBypasser interface {
	WithBypass(prefix string, fn func() error) error
}

// Map template names to their file paths relative to web/templates.
var templateFiles = map[string]string{
	"index":            "manager/index.html",
	"adminindex":       "manager/adminindex.html",
	"login":            "auth/login.html",
	"register":         "auth/register.html",
	"edit":             "auth/edit.html",
	"settings_account": "settings/account.html",
	"settings_content": "settings/content.html",
	"settings_admin":   "settings/admin.html",
	"create":           "manager/create.html",
	"addapp":           "manager/addapp.html",
	"configapp":        "manager/configapp.html",
	"uploadapp":        "manager/uploadapp.html",
	"firmware":         "manager/firmware.html",
	"update":           "manager/update.html",
	"device_tv":        "manager/device_tv.html",
	"settings":         "admin/settings.html",
}

func NewServer(db *gorm.DB, cfg *config.Settings) *Server {
	s := &Server{
		DB:          db,
		Router:      http.NewServeMux(),
		DataDir:     cfg.DataDir,
		Config:      cfg,
		Bundle:      i18n.NewBundle(language.English), // Default language
		Broadcaster: syncer.NewBroadcaster(),
		Upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		PromRegistry:          prometheus.DefaultRegisterer,
		PromGatherer:          prometheus.DefaultGatherer,
		externalProviders:     make(map[string]providers.ExternalAppProvider),
		appChunksCache:        make(map[string][]providers.RenderChunk),
		deviceChunkIndices:    make(map[string]int),
		deviceCurrentChunkApp: make(map[string]string),
		inFlightRenders:       make(map[string]bool),
		renderPending:         make(map[string]bool),
	}

	// Load Settings from DB
	// Secret Key
	secretKey, err := s.getSetting("secret_key")
	if err != nil || secretKey == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			slog.Error("Failed to generate random secret key", "error", err)
			// Fallback to avoid crash, though this is critical
			secretKey = "insecure-fallback-key-" + fmt.Sprintf("%d", time.Now().UnixNano())
		} else {
			secretKey = base64.StdEncoding.EncodeToString(b)
		}
		if err := s.setSetting("secret_key", secretKey); err != nil {
			slog.Error("Failed to save secret key to settings", "error", err)
		}
	}

	// System Repo
	repo, err := s.getSetting("system_apps_repo")
	if err == nil && repo != "" {
		cfg.SystemAppsRepo = repo
	}
	if val, err := s.getSetting(systemAppsAutoRefreshSettingKey); err != nil {
		slog.Warn("Failed to load system apps auto-refresh setting", "error", err)
	} else if val != "" {
		cfg.SystemAppsAutoRefresh = val == "true"
	}
	s.systemAppsAutoRefresh.Store(cfg.SystemAppsAutoRefresh)

	// Screeny Studio Settings from DB or env
	if val, err := s.getSetting("screeny_studio_url"); err == nil && val != "" {
		cfg.ScreenyStudioURL = val
	}
	if val, err := s.getSetting("screeny_scale_filter"); err == nil && val != "" {
		cfg.ScreenyScaleFilter = val
	}
	if cfg.ScreenyStudioURL != "" {
		s.RegisterExternalProvider(screeny.NewAdapter(screeny.AdapterConfig{
			StudioURL:    cfg.ScreenyStudioURL,
			FPS:          cfg.ScreenyFPS,
			MaxWebPBytes: cfg.ScreenyMaxWebPBytes,
			ScaleFilter:  cfg.ScreenyScaleFilter,
		}))
		slog.Info("Registered Screeny external app provider", "url", cfg.ScreenyStudioURL, "scaleFilter", cfg.ScreenyScaleFilter)
	}

	// Awtrix Settings from DB or env
	if val, err := s.getSetting("awtrix_url"); err == nil && val != "" {
		cfg.AwtrixURL = val
	}
	if val, err := s.getSetting("awtrix_scale_filter"); err == nil && val != "" {
		cfg.AwtrixScaleFilter = val
	}
	if val, err := s.getSetting("awtrix_layout"); err == nil && val != "" {
		cfg.AwtrixLayout = val
	}
	if cfg.AwtrixURL != "" {
		s.RegisterExternalProvider(awtrix.NewAdapter(awtrix.AdapterConfig{
			BaseURL:     cfg.AwtrixURL,
			FPS:         cfg.AwtrixFPS,
			ScaleFilter: cfg.AwtrixScaleFilter,
			Layout:      cfg.AwtrixLayout,
		}))
		slog.Info("Registered Awtrix external app provider", "url", cfg.AwtrixURL, "scaleFilter", cfg.AwtrixScaleFilter, "layout", cfg.AwtrixLayout)
	}

	// Load OIDC settings from DB
	if val, err := s.getSetting("oidc_enabled"); err == nil {
		cfg.OIDCEnabled = val == "true"
	}
	if val, err := s.getSetting("oidc_issuer_url"); err == nil {
		cfg.OIDCIssuerURL = val
	}
	if val, err := s.getSetting("oidc_client_id"); err == nil {
		cfg.OIDCClientID = val
	}
	if val, err := s.getSetting("oidc_client_secret"); err == nil {
		cfg.OIDCClientSecret = val
	}
	if val, err := s.getSetting("oidc_additional_scopes"); err == nil {
		cfg.OIDCAdditionalScopes = val
	}
	if val, err := s.getSetting("oidc_allow_auto_create"); err == nil {
		cfg.OIDCAllowAutoCreate = val == "true"
	}
	if val, err := s.getSetting("oidc_admin_group_claim"); err == nil {
		cfg.OIDCAdminGroupClaim = val
	}
	if val, err := s.getSetting("oidc_admin_group_value"); err == nil {
		cfg.OIDCAdminGroupValue = val
	}
	if val, err := s.getSetting("oidc_username_claim"); err == nil {
		cfg.OIDCUsernameClaim = val
	}

	// Initialize OIDC provider
	if cfg.OIDCEnabled && cfg.OIDCIssuerURL != "" {
		prov, err := s.setupOIDCProvider(context.Background())
		if err != nil {
			slog.Warn("Failed to initialize OIDC provider, OIDC login disabled", "error", err)
			s.OIDCProvider = nil
		} else {
			s.OIDCProvider = prov
			slog.Info("OIDC provider initialized", "issuer", cfg.OIDCIssuerURL)
		}
	}

	s.Store = sessions.NewCookieStore([]byte(secretKey))

	// Configure Session Store
	s.Store.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   86400 * 30,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	}

	gob.Register(webauthn.SessionData{})

	// Load translations
	s.Bundle.RegisterUnmarshalFunc("json", json.Unmarshal)

	if entries, err := web.Assets.ReadDir("i18n"); err != nil {
		slog.Error("Failed to read i18n directory", "error", err)
	} else {
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
				path := "i18n/" + entry.Name()
				data, err := web.Assets.ReadFile(path)
				if err != nil {
					slog.Error("Failed to read translation file", "file", entry.Name(), "error", err)
					continue
				}
				if _, err := s.Bundle.ParseMessageFileBytes(data, entry.Name()); err != nil {
					slog.Error("Failed to parse translation file", "file", entry.Name(), "error", err)
				}
			}
		}
	}

	funcMap := getFuncMap()

	// Load base templates and partials
	s.BaseTemplates = template.New("").Funcs(funcMap)
	// Parse base and partials
	if _, err := s.BaseTemplates.ParseFS(web.Assets, "templates/base.html", "templates/partials/*.html"); err != nil {
		slog.Error("Failed to parse base templates", "error", err)
	}

	s.PageTemplates = make(map[string]*template.Template)
	for name, path := range templateFiles {
		tmpl, err := s.BaseTemplates.Clone()
		if err != nil {
			slog.Error("Failed to clone template", "name", name, "error", err)
			continue
		}
		if _, err := tmpl.ParseFS(web.Assets, "templates/"+path); err != nil {
			slog.Error("Failed to parse page template", "name", name, "path", path, "error", err)
			continue
		}
		s.PageTemplates[name] = tmpl
	}

	s.registerMetrics()
	s.RefreshSystemAppsCache()

	// Clean up and recreate tmp directory
	tmpDir := s.GetTmpDir()
	if err := os.RemoveAll(tmpDir); err != nil {
		slog.Warn("Failed to clean up tmp directory on startup", "path", tmpDir, "error", err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		slog.Error("Failed to create tmp directory on startup", "path", tmpDir, "error", err)
	}

	go s.checkForUpdates(context.Background())
	go s.autoRefreshSystemRepo(context.Background())
	go s.autoRefreshCustomAppsRepos()
	go s.preheatExternalAppThumbnails()
	if cfg.NibletCloudURL != "" {
		go s.runNibletSync()
		go s.runUsageSync()
	}

	s.routes()
	return s
}

func (s *Server) routes() {
	// Conflict Resolution Handlers (Must be registered before conflicting wildcards?)
	// Actually order of registration doesn't matter in Go 1.22 for correctness, but presence matters.
	// But let's put them first for clarity.
	s.Router.HandleFunc("GET /static/ws", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })

	// Static files
	staticFS, err := fs.Sub(web.Assets, "static")
	if err != nil {
		slog.Error("Failed to sub static fs", "error", err)
	} else {
		fileServer := http.FileServer(http.FS(staticFS))
		// Register specific subdirectories to avoid conflict with /{id}/ws
		s.Router.Handle("GET /static/css/", http.StripPrefix("/static/", fileServer))
		s.Router.Handle("GET /static/js/", http.StripPrefix("/static/", fileServer))
		s.Router.Handle("GET /static/webfonts/", http.StripPrefix("/static/", fileServer))
		s.Router.Handle("GET /static/images/", http.StripPrefix("/static/", fileServer))
		s.Router.Handle("GET /static/favicon.ico", http.StripPrefix("/static/", fileServer))
	}

	// Serve firmware binaries
	firmwareDir := filepath.Join(s.DataDir, "firmware")
	s.Router.Handle("GET /static/firmware/", http.StripPrefix("/static/firmware/", http.FileServer(http.Dir(firmwareDir))))

	// App Preview (Specific path)
	s.Router.HandleFunc("GET /preview/app/{id}", s.RequireLogin(s.handleAppThumbnail))

	s.SetupAPIRoutes()
	s.SetupAuthRoutes()

	// Device endpoint
	s.Router.Handle("GET /{id}/next", http.HandlerFunc(s.handleNextApp))

	// Web UI
	s.Router.HandleFunc("GET /", s.RequireLogin(s.handleIndex))
	s.Router.HandleFunc("GET /admin", s.RequireLogin(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/settings/admin", http.StatusSeeOther)
	}))
	s.Router.HandleFunc("GET /settings/admin", s.RequireLogin(s.handleAdminIndex))
	s.Router.HandleFunc("POST /settings/admin/oidc", s.RequireLogin(s.handleAdminSettingsPost))
	s.Router.Handle("POST /settings/admin/usage-sharing", http.NewCrossOriginProtection().Handler(s.RequireLogin(s.handleUsageSharingConsent)))
	s.Router.HandleFunc("GET /.well-known/niblet-reporter", s.handleUsageChallenge)
	s.Router.HandleFunc("GET /admin/settings", s.RequireLogin(s.handleAdminSettingsGet))   // Keep old path just in case
	s.Router.HandleFunc("POST /admin/settings", s.RequireLogin(s.handleAdminSettingsPost)) // Keep old path just in case
	s.Router.HandleFunc("DELETE /admin/users/{username}", s.RequireLogin(s.handleDeleteUser))
	s.Router.HandleFunc("DELETE /settings/admin/users/{username}", s.RequireLogin(s.handleDeleteUser))
	s.Router.HandleFunc("POST /settings/admin/users/{username}/email", s.RequireLogin(s.handleAdminUpdateUserEmail))

	s.Router.HandleFunc("GET /devices/create", s.RequireLogin(s.handleCreateDeviceGet))
	s.Router.HandleFunc("POST /devices/create", s.RequireLogin(s.handleCreateDevicePost))
	s.Router.HandleFunc("POST /devices/import", s.RequireLogin(s.handleImportNewDeviceConfig))

	s.Router.HandleFunc("POST /devices/{id}/update_brightness", s.RequireLogin(s.RequireDevice(s.handleUpdateBrightness)))
	s.Router.HandleFunc("POST /devices/{id}/update_interval", s.RequireLogin(s.RequireDevice(s.handleUpdateInterval)))
	s.Router.HandleFunc("POST /devices/{id}/set_night_mode_override", s.RequireLogin(s.RequireDevice(s.handleSetNightModeOverride)))
	s.Router.HandleFunc("POST /devices/{id}/set_dim_mode_override", s.RequireLogin(s.RequireDevice(s.handleSetDimModeOverride)))

	s.Router.HandleFunc("GET /devices/{id}/addapp", s.RequireLogin(s.RequireDevice(s.handleAddAppGet)))
	s.Router.HandleFunc("POST /devices/{id}/addapp", s.RequireLogin(s.RequireDevice(s.handleAddAppPost)))

	s.Router.HandleFunc("POST /devices/{id}/{iname}/delete", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleDeleteApp))))
	s.Router.HandleFunc("GET /devices/{id}/{iname}/config", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleConfigAppGet))))
	s.Router.HandleFunc("GET /devices/{id}/{iname}/schema", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleAppSchemaGet))))
	s.Router.HandleFunc("POST /devices/{id}/{iname}/config", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleConfigAppPost))))
	s.Router.HandleFunc("POST /devices/{id}/{iname}/schema_handler/{handler}", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleSchemaHandler))))

	s.Router.HandleFunc("POST /devices/{id}/{iname}/toggle_pin", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleTogglePin))))
	s.Router.HandleFunc("POST /devices/{id}/{iname}/toggle_enabled", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleToggleEnabled))))
	s.Router.HandleFunc("POST /devices/{id}/{iname}/moveapp", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleMoveApp))))
	s.Router.HandleFunc("POST /devices/{id}/{iname}/duplicate", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleDuplicateApp))))
	s.Router.HandleFunc("POST /devices/{id}/apps/duplicate_from/{source_device_id}/{iname}", s.RequireLogin(s.RequireDevice(s.handleDuplicateAppToDevice)))
	s.Router.HandleFunc("POST /devices/{id}/reorder_apps", s.RequireLogin(s.RequireDevice(s.handleReorderApps)))

	s.Router.HandleFunc("GET /devices/{id}/uploadapp", s.RequireLogin(s.RequireDevice(s.handleUploadAppGet)))
	s.Router.HandleFunc("POST /devices/{id}/uploadapp", s.RequireLogin(s.RequireDevice(s.handleUploadAppPost)))
	s.Router.HandleFunc("GET /devices/{id}/uploads/{filename}/delete", s.RequireLogin(s.RequireDevice(s.handleDeleteUpload)))

	s.Router.HandleFunc("POST /set_theme_preference", s.RequireLogin(s.handleSetThemePreference))
	s.Router.HandleFunc("POST /set_user_repo", s.RequireLogin(s.handleSetUserRepo))
	s.Router.HandleFunc("POST /refresh_user_repo", s.RequireLogin(s.handleRefreshUserRepo))

	s.Router.HandleFunc("GET /export_user_config", s.RequireLogin(s.handleExportUserConfig))
	s.Router.HandleFunc("POST /import_user_config", s.RequireLogin(s.handleImportUserConfig))
	s.Router.HandleFunc("GET /devices/{id}/export_config", s.RequireLogin(s.RequireDevice(s.handleExportDeviceConfig)))

	s.Router.HandleFunc("POST /set_system_repo", s.RequireLogin(s.handleSetSystemRepo))
	s.Router.HandleFunc("POST /refresh_system_repo", s.RequireLogin(s.handleRefreshSystemRepo))
	s.Router.HandleFunc("POST /settings/system-apps-auto-refresh", s.RequireLogin(s.handleSetSystemAppsAutoRefresh))
	s.Router.HandleFunc("POST /update_firmware", s.RequireLogin(s.handleUpdateFirmware))
	s.Router.HandleFunc("POST /settings/screeny", s.RequireLogin(s.handleSetScreenyConfig))
	s.Router.HandleFunc("POST /settings/awtrix", s.RequireLogin(s.handleSetAwtrixConfig))

	// App broken status (development only)
	s.Router.HandleFunc("POST /mark_app_broken", s.RequireLogin(s.handleMarkAppBroken))
	s.Router.HandleFunc("POST /unmark_app_broken", s.RequireLogin(s.handleUnmarkAppBroken))

	s.Router.HandleFunc("GET /devices/{id}/current", s.RequireLogin(s.RequireDevice(s.handleCurrentApp)))
	s.Router.HandleFunc("GET /devices/{id}/installations/{iname}/preview", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handleRenderConfigPreview))))
	s.Router.HandleFunc("POST /devices/{id}/{iname}/preview", s.RequireLogin(s.RequireDevice(s.RequireApp(s.handlePushPreview))))

	// Firmware
	s.Router.HandleFunc("GET /devices/{id}/firmware", s.RequireLogin(s.RequireDevice(s.handleFirmwareGenerateGet)))
	s.Router.HandleFunc("POST /devices/{id}/firmware", s.RequireLogin(s.RequireDevice(s.handleFirmwareGeneratePost)))
	s.Router.HandleFunc("POST /devices/{id}/ota", s.RequireLogin(s.RequireDevice(s.handleTriggerOTA))) // OTA Update

	s.Router.HandleFunc("GET /devices/{id}/update", s.RequireLogin(s.RequireDevice(s.handleUpdateDeviceGet)))
	s.Router.HandleFunc("POST /devices/{id}/update", s.RequireLogin(s.RequireDevice(s.handleUpdateDevicePost)))
	s.Router.HandleFunc("POST /devices/{id}/delete", s.RequireLogin(s.RequireDevice(s.handleDeleteDevice)))
	s.Router.HandleFunc("POST /devices/{id}/import_config", s.RequireLogin(s.RequireDevice(s.handleImportDeviceConfig)))
	s.Router.HandleFunc("POST /devices/{id}/reboot", s.RequireLogin(s.RequireDevice(s.handleRebootDevice)))

	s.Router.HandleFunc("POST /devices/{id}/update_firmware_settings", s.RequireLogin(s.RequireDevice(s.handleUpdateFirmwareSettings)))

	// Add this line for the TV View endpoint
	s.Router.HandleFunc("GET /devices/{id}/tv", s.RequireLogin(s.RequireDevice(s.handleDeviceTV)))

	// Websocket routes
	s.SetupWebsocketRoutes()

	// Health and Metrics
	s.Router.HandleFunc("GET /health", s.handleHealth)
	s.Router.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(s.PromGatherer, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})
	s.Router.HandleFunc("GET /dots", s.handleDots)

	if s.Config.EnablePprof {
		slog.Info("Enabling pprof", "path", "/debug/pprof/")
		s.Router.HandleFunc("GET /debug/pprof/", pprof.Index)
		s.Router.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		s.Router.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		s.Router.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		s.Router.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Chain middlewares: Recover -> Metrics -> Gzip -> Logging -> Proxy -> Mux
	RecoverMiddleware(s.metricsMiddleware(GzipMiddleware(LoggingMiddleware(ProxyMiddleware(s.Router))))).ServeHTTP(w, r)
}

func (s *Server) GetTmpDir() string {
	return filepath.Join(s.DataDir, "tmp")
}

func (s *Server) handleDots(w http.ResponseWriter, r *http.Request) {
	widthStr := r.URL.Query().Get("w")
	heightStr := r.URL.Query().Get("h")
	radiusStr := r.URL.Query().Get("r")

	width := 64
	height := 32
	radius := 0.3

	if wVal, err := strconv.Atoi(widthStr); err == nil && wVal > 0 {
		width = wVal
	}
	if hVal, err := strconv.Atoi(heightStr); err == nil && hVal > 0 {
		height = hVal
	}
	if rVal, err := strconv.ParseFloat(radiusStr, 64); err == nil && rVal > 0 {
		radius = rVal
	}

	etag := fmt.Sprintf("\"%d-%d-%g\"", width, height, radius)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age=31536000")

	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "image/svg+xml")

	rStr := strings.TrimPrefix(fmt.Sprintf("%g", radius), "0")
	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" fill="#fff"><defs><pattern id="dot" width="1" height="1" patternUnits="userSpaceOnUse"><circle cx=".5" cy=".5" r="%s"/></pattern></defs><rect width="100%%" height="100%%" fill="url(#dot)"/></svg>`, width, height, rStr)

	if _, err := w.Write([]byte(svg)); err != nil {
		slog.Error("Failed to write dots SVG", "error", err)
	}
}

// RegisterExternalProvider registers an external app store/renderer provider.
func (s *Server) RegisterExternalProvider(p providers.ExternalAppProvider) {
	s.externalProvidersMu.Lock()
	defer s.externalProvidersMu.Unlock()
	s.externalProviders[p.Name()] = p
}

// GetExternalProvider retrieves an external app provider by name.
func (s *Server) GetExternalProvider(name string) providers.ExternalAppProvider {
	s.externalProvidersMu.RLock()
	defer s.externalProvidersMu.RUnlock()
	return s.externalProviders[name]
}

// ListExternalProviders returns all registered external app providers.
func (s *Server) ListExternalProviders() []providers.ExternalAppProvider {
	s.externalProvidersMu.RLock()
	defer s.externalProvidersMu.RUnlock()
	list := make([]providers.ExternalAppProvider, 0, len(s.externalProviders))
	for _, p := range s.externalProviders {
		list = append(list, p)
	}
	return list
}

// updateScreenyConfig updates Screeny settings in the DB and running server.
func (s *Server) updateScreenyConfig(studioURL, scaleFilter string) error {
	cleanURL := strings.TrimRight(strings.TrimSpace(studioURL), "/")
	s.Config.ScreenyStudioURL = cleanURL
	if scaleFilter != "" {
		s.Config.ScreenyScaleFilter = scaleFilter
	}
	if err := s.setSetting("screeny_studio_url", s.Config.ScreenyStudioURL); err != nil {
		return err
	}
	if err := s.setSetting("screeny_scale_filter", s.Config.ScreenyScaleFilter); err != nil {
		return err
	}

	if s.Config.ScreenyStudioURL != "" {
		adapter := screeny.NewAdapter(screeny.AdapterConfig{
			StudioURL:    s.Config.ScreenyStudioURL,
			FPS:          s.Config.ScreenyFPS,
			MaxWebPBytes: s.Config.ScreenyMaxWebPBytes,
			ScaleFilter:  s.Config.ScreenyScaleFilter,
		})
		s.RegisterExternalProvider(adapter)
		slog.Info("Updated Screeny external app provider", "url", s.Config.ScreenyStudioURL, "scaleFilter", s.Config.ScreenyScaleFilter)
		go s.preheatExternalAppThumbnails()
	} else {
		s.externalProvidersMu.Lock()
		delete(s.externalProviders, "screeny")
		s.externalProvidersMu.Unlock()
		slog.Info("Deregistered Screeny external app provider")
	}
	return nil
}

// updateAwtrixConfig updates Awtrix settings in the DB and running server.
func (s *Server) updateAwtrixConfig(awtrixURL, scaleFilter, layout string) error {
	cleanURL := strings.TrimRight(strings.TrimSpace(awtrixURL), "/")
	s.Config.AwtrixURL = cleanURL
	if scaleFilter != "" {
		s.Config.AwtrixScaleFilter = scaleFilter
	}
	if layout != "" {
		s.Config.AwtrixLayout = layout
	}
	if err := s.setSetting("awtrix_url", s.Config.AwtrixURL); err != nil {
		return err
	}
	if err := s.setSetting("awtrix_scale_filter", s.Config.AwtrixScaleFilter); err != nil {
		return err
	}
	if err := s.setSetting("awtrix_layout", s.Config.AwtrixLayout); err != nil {
		return err
	}

	if s.Config.AwtrixURL != "" {
		adapter := awtrix.NewAdapter(awtrix.AdapterConfig{
			BaseURL:     s.Config.AwtrixURL,
			FPS:         s.Config.AwtrixFPS,
			ScaleFilter: s.Config.AwtrixScaleFilter,
			Layout:      s.Config.AwtrixLayout,
		})
		s.RegisterExternalProvider(adapter)
		slog.Info("Updated Awtrix external app provider", "url", s.Config.AwtrixURL, "scaleFilter", s.Config.AwtrixScaleFilter, "layout", s.Config.AwtrixLayout)
		go s.preheatExternalAppThumbnails()
	} else {
		s.externalProvidersMu.Lock()
		delete(s.externalProviders, "awtrix")
		s.externalProvidersMu.Unlock()
		slog.Info("Deregistered Awtrix external app provider")
	}
	return nil
}

// isExternalAppPath returns true if the path refers to an external provider (e.g. "screeny:clock").
func (s *Server) isExternalAppPath(path string) bool {
	_, _, ok := s.parseExternalAppPath(path)
	return ok
}

// parseExternalAppPath splits a path like "screeny:split_flap" into (provider, "split_flap", true).
func (s *Server) parseExternalAppPath(path string) (providers.ExternalAppProvider, string, bool) {
	parts := strings.SplitN(path, ":", 2)
	if len(parts) != 2 {
		return nil, "", false
	}
	provider := s.GetExternalProvider(parts[0])
	if provider == nil {
		return nil, "", false
	}
	return provider, parts[1], true
}

// getExternalProviderFPS returns the configured frame rate for a provider.
func (s *Server) getExternalProviderFPS(provider providers.ExternalAppProvider) int {
	if provider != nil && provider.Name() == "awtrix" {
		if s.Config.AwtrixFPS > 0 {
			return s.Config.AwtrixFPS
		}
		return 15
	}
	if s.Config.ScreenyFPS > 0 {
		return s.Config.ScreenyFPS
	}
	return 30
}

// GetMaxChunkSizeForDevice returns the chunk size budget based on device type.
func (s *Server) GetMaxChunkSizeForDevice(device *data.Device) int {
	if device == nil {
		if s.Config.ScreenyMaxWebPBytesESP > 0 {
			return s.Config.ScreenyMaxWebPBytesESP
		}
		return 160000
	}

	// 1. Raspberry Pi / Tronberry clients have expanded Linux memory capabilities (default: 5 MB).
	if device.Type == data.DeviceRaspberryPi ||
		device.Type == data.DeviceRaspberryPiWide ||
		device.Type == data.DeviceRaspberryPiSquare ||
		strings.EqualFold(device.Info.FirmwareType, "Tronberry") {
		if s.Config.ScreenyMaxWebPBytesRPi > 0 {
			return s.Config.ScreenyMaxWebPBytesRPi
		}
		return s.Config.ScreenyMaxWebPBytes
	}

	// 2. Pixoticker has an ultra-compact memory buffer (60,000 bytes in firmware).
	if device.Type == data.DevicePixoticker {
		return 50000
	}

	// 3. ESP32-S3 and Wide devices (Matrix Wide, Tronbyt S3 Wide, MatrixPortal S3, Tidbyt Gen 2)
	// have larger 460,000-byte firmware buffers and expanded PSRAM/SRAM.
	if device.Type == data.DeviceTronbytS3Wide ||
		device.Type == data.DeviceMatrixPortalWide ||
		device.Type == data.DeviceMatrixPortalSquare ||
		device.Type == data.DeviceTronbytS3 ||
		device.Type == data.DeviceMatrixPortal ||
		device.Type == data.DeviceWaveshareS3 ||
		device.Type == data.DeviceTidbytGen2 {
		return 450000
	}

	// 4. Tidbyt Gen 1 and Waveshare MatrixPortal have classic ESP32 chips with strict heap constraints.
	// 160,000 bytes (160 KB) is the safe maximum chunk size that accommodates double-buffering
	// without heap fragmentation or buffer overflow (chunks near 200 KB cause heap exhaustion).
	if s.Config.ScreenyMaxWebPBytesESP > 0 {
		return s.Config.ScreenyMaxWebPBytesESP
	}
	return 160000
}

// GetMaxChunkDurationForDevice returns the maximum duration in ms per chunk based on device type.
// For ESP32-based devices (e.g. Tidbyt Gen1/Gen2, Tronbyt S3, MatrixPortal), large WebPs with
// hundreds of frames overwhelm the hardware demuxer and memory. Capping chunks at 5000ms (5s)
// ensures smooth sequential playback without device buffer exhaustion.
// For Raspberry Pi / Tronberry devices, 0 is returned (no duration limit).
func (s *Server) GetMaxChunkDurationForDevice(device *data.Device) int {
	if device == nil {
		return 5000
	}
	if device.Type == data.DeviceRaspberryPi ||
		device.Type == data.DeviceRaspberryPiWide ||
		device.Type == data.DeviceRaspberryPiSquare ||
		strings.EqualFold(device.Info.FirmwareType, "Tronberry") {
		return 0
	}
	return 5000
}

func (s *Server) invalidateAppChunks(deviceID string, iname string) {
	key := s.getAppChunksCacheKey(deviceID, iname)
	s.appChunksMu.Lock()
	delete(s.appChunksCache, key)
	s.appChunksMu.Unlock()
}

type chunkMeta struct {
	DurationMs int    `json:"duration_ms"`
	File       string `json:"file"`
}

func (s *Server) getAppChunksCacheKey(deviceID, iname string) string {
	return deviceID + ":" + iname
}

func (s *Server) getAppChunks(deviceID string, app *data.App) []providers.RenderChunk {
	if app == nil {
		return nil
	}
	key := s.getAppChunksCacheKey(deviceID, app.Iname)
	s.appChunksMu.RLock()
	if cached, ok := s.appChunksCache[key]; ok && len(cached) > 0 {
		s.appChunksMu.RUnlock()
		return cached
	}
	s.appChunksMu.RUnlock()

	// Try loading from disk
	deviceWebpDir, err := s.ensureDeviceImageDir(deviceID)
	if err != nil {
		return nil
	}
	appBasename := fmt.Sprintf("%s-%s", app.Name, app.Iname)
	metaFile := filepath.Join(deviceWebpDir, fmt.Sprintf("%s_chunks.json", appBasename))
	metaBytes, err := os.ReadFile(metaFile)
	if err == nil {
		var metas []chunkMeta
		if err := json.Unmarshal(metaBytes, &metas); err == nil && len(metas) > 0 {
			var chunks []providers.RenderChunk
			for _, m := range metas {
				chunkPath := filepath.Join(deviceWebpDir, m.File)
				b, err := os.ReadFile(chunkPath)
				if err != nil {
					chunks = nil
					break
				}
				chunks = append(chunks, providers.RenderChunk{
					WebPBytes:  b,
					DurationMs: m.DurationMs,
				})
			}
			if len(chunks) > 0 {
				s.appChunksMu.Lock()
				s.appChunksCache[key] = chunks
				s.appChunksMu.Unlock()
				return chunks
			}
		}
	}

	// Fallback: check if single .webp exists
	singlePath := s.getAppWebpPath(deviceWebpDir, app)
	if b, err := os.ReadFile(singlePath); err == nil && len(b) > 0 {
		durationMs := app.DisplayTime * 1000
		if durationMs <= 0 {
			durationMs = 15000
		}
		chunks := []providers.RenderChunk{
			{
				WebPBytes:  b,
				DurationMs: durationMs,
			},
		}
		s.appChunksMu.Lock()
		s.appChunksCache[key] = chunks
		s.appChunksMu.Unlock()
		return chunks
	}

	return nil
}

func (s *Server) saveAppChunks(deviceID string, app *data.App, chunks []providers.RenderChunk) error {
	if app == nil || len(chunks) == 0 {
		return nil
	}
	deviceWebpDir, err := s.ensureDeviceImageDir(deviceID)
	if err != nil {
		return err
	}
	appBasename := fmt.Sprintf("%s-%s", app.Name, app.Iname)

	// Remove old chunk files for this app to prevent orphaned chunks if chunk count changed
	oldChunks, _ := filepath.Glob(filepath.Join(deviceWebpDir, fmt.Sprintf("%s_chunk_*.webp", appBasename)))
	for _, oldChunk := range oldChunks {
		_ = os.Remove(oldChunk)
	}

	// The primary image (<appBasename>.webp) is what devices fetch on rotation or fallback.
	// It must always be safe for device hardware and stay within maxChunkSize, so we
	// write chunk 0 (the first chunk) to primaryPath.
	primaryPath := s.getAppWebpPath(deviceWebpDir, app)
	primaryBytes := chunks[0].WebPBytes
	if err := os.WriteFile(primaryPath, primaryBytes, 0644); err != nil {
		return err
	}

	// Save the complete unchunked animation (if available) to <appBasename>_full.webp
	// for rich web previews and dashboard display.
	if len(chunks[0].FullWebPBytes) > 0 {
		fullPath := filepath.Join(deviceWebpDir, fmt.Sprintf("%s_full.webp", appBasename))
		if err := os.WriteFile(fullPath, chunks[0].FullWebPBytes, 0644); err != nil {
			slog.Error("Failed to write full webp preview file", "path", fullPath, "error", err)
		}
	}

	// If multiple chunks, write individual chunk files and chunk metadata JSON
	var metas []chunkMeta
	for i, chunk := range chunks {
		fileName := fmt.Sprintf("%s_chunk_%d.webp", appBasename, i)
		chunkPath := filepath.Join(deviceWebpDir, fileName)
		if err := os.WriteFile(chunkPath, chunk.WebPBytes, 0644); err != nil {
			slog.Error("Failed to write chunk file", "path", chunkPath, "error", err)
		}
		metas = append(metas, chunkMeta{
			DurationMs: chunk.DurationMs,
			File:       fileName,
		})
	}

	metaFile := filepath.Join(deviceWebpDir, fmt.Sprintf("%s_chunks.json", appBasename))
	metaBytes, err := json.Marshal(metas)
	if err == nil {
		_ = os.WriteFile(metaFile, metaBytes, 0644)
	}

	// Store in memory cache
	key := s.getAppChunksCacheKey(deviceID, app.Iname)
	s.appChunksMu.Lock()
	s.appChunksCache[key] = chunks
	s.appChunksMu.Unlock()

	return nil
}
