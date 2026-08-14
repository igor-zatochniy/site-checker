package main

import (
	"log/slog"
	"os"

	"github.com/igor-zatochniy/site-checker/internal/sitechecker"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	os.Exit(run())
}

func run() int {
	if err := sitechecker.Main(version, commit, buildDate); err != nil {
		slog.Error("Site Checker stopped with an error", "error", err)
		return 1
	}
	return 0
}
