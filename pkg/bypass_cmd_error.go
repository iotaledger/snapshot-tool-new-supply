package pkg

import (
	"os"

	"github.com/spf13/pflag"
)

func init() {
	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
}
