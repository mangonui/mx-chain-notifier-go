package gin

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/multiversx/mx-chain-communication-go/websocket"
	"github.com/multiversx/mx-chain-core-go/core/check"
	logger "github.com/multiversx/mx-chain-logger-go"
	apiErrors "github.com/multiversx/mx-chain-notifier-go/api/errors"
	"github.com/multiversx/mx-chain-notifier-go/api/groups"
	"github.com/multiversx/mx-chain-notifier-go/api/shared"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/config"
)

const (
	defaultRestInterface = "localhost:5000"
)

var log = logger.GetOrCreate("api/gin")

const (
	eventsGroupID = "events"
	hubGroupID    = "hub"
)

// ArgsWebServerHandler holds the arguments needed to create a web server handler
type ArgsWebServerHandler struct {
	Facade         shared.FacadeHandler
	PayloadHandler websocket.PayloadHandler
	Configs        config.Configs
}

// webServer is a wrapper for gin.Engine, holding additional components
type webServer struct {
	sync.RWMutex
	facade         shared.FacadeHandler
	payloadHandler websocket.PayloadHandler
	httpServer     shared.HTTPServerCloser
	groups         map[string]shared.GroupHandler
	configs        config.Configs
	wasTriggered   bool
	cancelFunc     func()
}

// NewWebServerHandler creates and configures an instance of webServer
func NewWebServerHandler(args ArgsWebServerHandler) (*webServer, error) {
	err := checkArgs(args)
	if err != nil {
		return nil, err
	}

	return &webServer{
		facade:         args.Facade,
		payloadHandler: args.PayloadHandler,
		configs:        args.Configs,
		groups:         make(map[string]shared.GroupHandler),
		wasTriggered:   false,
	}, nil
}

func checkArgs(args ArgsWebServerHandler) error {
	if check.IfNil(args.Facade) {
		return apiErrors.ErrNilFacadeHandler
	}
	if args.Configs.Flags.PublisherType == "" {
		return common.ErrInvalidAPIType
	}
	if check.IfNil(args.PayloadHandler) {
		return apiErrors.ErrNilPayloadHandler
	}

	return nil
}

func (w *webServer) getWSAddr() string {
	addr := w.configs.MainConfig.ConnectorApi.Host
	if addr == "" {
		return defaultRestInterface
	}

	if !strings.Contains(addr, ":") {
		return fmt.Sprintf(":%s", addr)
	}

	return addr
}

// Run starts the server and the Hub as goroutines
// It returns an instance of http.Server
func (w *webServer) Run() error {
	w.Lock()
	defer w.Unlock()

	var err error

	if w.wasTriggered == true {
		log.Error("Web server has been already triggered successfuly once")
		return nil
	}

	engine := gin.Default()
	// ISSUE-015: previously this was `cors.Default()` which sets
	// AllowAllOrigins=true. The notifier streams transactional event
	// data — a permissive CORS posture lets browser-adjacent attackers
	// read that stream cross-origin. Mirror the indexer/chain-go pattern
	// with a localhost-only AllowOriginFunc. Operators that need
	// cross-origin event consumption should put the notifier behind an
	// auth-aware reverse proxy rather than relaxing this here.
	corsCfg := cors.DefaultConfig()
	corsCfg.AllowOriginFunc = isAllowedCORSOrigin
	corsCfg.AddAllowHeaders("Authorization")
	engine.Use(cors.New(corsCfg))

	err = w.createGroups()
	if err != nil {
		return err
	}

	w.registerRoutes(engine)

	addr := w.getWSAddr()

	// ISSUE-017: previously only ReadHeaderTimeout was set, leaving the
	// notifier vulnerable to slow-body, slow-write and idle-keepalive
	// resource exhaustion. The notifier streams events so WriteTimeout
	// must accommodate buffered fanout — 60s is comfortably above
	// observed event-burst sizes.
	server := &http.Server{
		Addr:              addr,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	w.httpServer, err = NewHTTPServerWrapper(server)
	if err != nil {
		return err
	}

	go w.httpServer.Start()

	w.wasTriggered = true

	return nil
}

func (w *webServer) createGroups() error {
	groupsMap := make(map[string]shared.GroupHandler)

	eventsGroupArgs := groups.ArgsEventsGroup{
		Facade:         w.facade,
		PayloadHandler: w.payloadHandler,
	}

	if w.configs.MainConfig.ConnectorApi.Enabled {
		eventsGroup, err := groups.NewEventsGroup(eventsGroupArgs)
		if err != nil {
			return err
		}
		groupsMap[eventsGroupID] = eventsGroup
	}

	statusGroup, err := groups.NewStatusGroup(w.facade)
	if err != nil {
		return err
	}
	groupsMap["status"] = statusGroup

	if w.configs.Flags.PublisherType == common.WSPublisherType {
		hubHandler, err := groups.NewHubGroup(w.facade)
		if err != nil {
			return err
		}
		groupsMap[hubGroupID] = hubHandler
	}

	w.groups = groupsMap

	return nil
}

func (w *webServer) registerRoutes(ginEngine *gin.Engine) {
	for groupName, groupHandler := range w.groups {
		log.Info("registering API group", "group name", groupName)

		ginGroup := ginEngine.Group(fmt.Sprintf("/%s", groupName))

		groupHandler.RegisterRoutes(ginGroup, w.configs.ApiRoutesConfig)
	}
}

// Close will handle the closing of inner components
func (w *webServer) Close() error {
	if w.cancelFunc != nil {
		w.cancelFunc()
	}

	w.Lock()
	err := w.httpServer.Close()
	w.Unlock()

	if err != nil {
		err = fmt.Errorf("%w while closing the http server in gin/webServer", err)
	}

	return err
}

// IsInterfaceNil returns true if there is no value under the interface
func (w *webServer) IsInterfaceNil() bool {
	return w == nil
}

// isAllowedCORSOrigin permits only same-host (loopback) Origins. See
// issues/ISSUE-015 and the mirroring helper in mx-chain-es-indexer-go.
func isAllowedCORSOrigin(origin string) bool {
	parsedOrigin, err := url.Parse(origin)
	if err != nil {
		return false
	}
	hostname := strings.ToLower(parsedOrigin.Hostname())
	return hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
}
