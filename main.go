package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/cristalhq/appx"
	"github.com/cristalhq/synx"
)

func main() {
	ctx := appx.Context()

	if err := run(ctx, os.Args[1:]); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, args []string) error {
	fset := flag.NewFlagSet("didis", flag.ContinueOnError)

	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("flag parse: %w", err)
	}

	cg := synx.NewContextGroup(ctx)

	cg.Go(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	if err := cg.WaitErr(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
