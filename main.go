package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/cristaloleg/didis/internal/core"
	"github.com/cristaloleg/didis/internal/inmem"
	"github.com/cristaloleg/didis/internal/server"

	"github.com/cristalhq/appx"
	"github.com/cristalhq/synx"
)

type Config struct {
	Port int `json:"port" yaml:"port"`
}

func main() {
	ctx := appx.Context()

	if err := run(ctx, os.Args[1:]); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, args []string) error {
	var cfg Config

	fset := flag.NewFlagSet("didis", flag.ContinueOnError)
	fset.IntVar(&cfg.Port, "port", 26379, "port on which server will start")

	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("flag parse: %w", err)
	}

	var store core.Store
	store = inmem.New()

	srvCfg := server.Config{
		Addr:  "localhost:" + strconv.Itoa(cfg.Port),
		Store: store,
	}

	srv, err := server.New(srvCfg)
	if err != nil {
		return fmt.Errorf("server init: %w", err)
	}

	cg := synx.NewContextGroup(ctx)
	cg.Go(srv.Run)

	if err := cg.WaitErr(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
