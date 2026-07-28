package main

import "github.com/igor-zatochniy/site-checker/internal/sitechecker"

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	sitechecker.Main(version, commit, buildDate)
}
