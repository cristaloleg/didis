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
	"github.com/cristaloleg/didis/internal/ondisk"
	"github.com/cristaloleg/didis/internal/server"

	"github.com/cristalhq/appx"
	"github.com/cristalhq/synx"
)

type Config struct {
	Port   int    `json:"port" yaml:"port"`
	Inmem  bool   `json:"inmem" yaml:"inmem"`
	Dir    string `json:"dir" yaml:"dir"`
	NoSync bool   `json:"nosync" yaml:"nosync"`
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
	fset.BoolVar(&cfg.Inmem, "inmem", false, "store data only in memory (gone after server stop)")
	fset.StringVar(&cfg.Dir, "dir", ".didis", "dir where data will be located")
	fset.BoolVar(&cfg.NoSync, "nosync", false, "do not call sync after each write")

	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("flag parse: %w", err)
	}

	var store core.Store
	var err error

	if cfg.Inmem {
		store = inmem.New()
	} else {
		dbCfg := ondisk.Config{
			Dir:    cfg.Dir,
			NoSync: cfg.NoSync,
		}

		store, err = ondisk.Open(dbCfg)
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
	}

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
