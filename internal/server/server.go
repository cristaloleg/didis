package server

import (
	"context"
	"fmt"
	"net"

	"github.com/cristaloleg/didis/internal/core"

	"github.com/tidwall/redcon"
)

type Server struct {
	cfg Config
	db  core.Store

	addr string
	ln   net.Listener
	mux  *redcon.ServeMux
	srv  *redcon.Server
}

type Config struct {
	Addr string `json:"addr" yaml:"addr"`

	Store core.Store `json:"-" yaml:"-"`
}

func New(cfg Config) (*Server, error) {
	s := &Server{
		cfg: cfg,
		db:  cfg.Store,
	}

	var err error
	s.ln, err = net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	s.addr = s.ln.Addr().String()

	s.srv = redcon.NewServer(
		s.cfg.Addr,
		s.makeMux().ServeRESP,
		s.onAccept,
		s.onClosed,
	)

	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	go func() {
		s.srv.Serve(s.ln)
	}()

	<-ctx.Done()
	if err := s.ln.Close(); err != nil {
		return fmt.Errorf("listen close: %w", err)
	}
	return nil
}

func (s *Server) onAccept(conn redcon.Conn) bool {
	return true
}

func (s *Server) onClosed(conn redcon.Conn, err error) {}

func (s *Server) makeMux() *redcon.ServeMux {
	mux := redcon.NewServeMux()

	mux.HandleFunc("append", s.handleAPPEND)
	mux.HandleFunc("decr", s.handleDECR)
	mux.HandleFunc("decrby", s.handleDECRBY)
	mux.HandleFunc("get", s.handleGET)
	mux.HandleFunc("getdel", s.handleGETDEL)
	mux.HandleFunc("getrange", s.handleGETRANGE)
	mux.HandleFunc("getset", s.handleGETSET)
	mux.HandleFunc("incr", s.handleINCR)
	mux.HandleFunc("incrby", s.handleINCRBY)
	mux.HandleFunc("incrbyfloat", s.handleINCRBYFLOAT)
	mux.HandleFunc("mget", s.handleMGET)
	mux.HandleFunc("mset", s.handleMSET)
	mux.HandleFunc("set", s.handleSET)
	mux.HandleFunc("strlen", s.handleSTRLEN)

	return mux
}
