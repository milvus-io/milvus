package command

import (
	"flag"

	"github.com/milvus-io/milvus/cmd/tools/migration/console"
)

type commandParser struct {
	configYaml string
}

func (c *commandParser) formatYaml(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.configYaml, "config", "", "set config yaml")
}

func (c *commandParser) parse(args []string, flags *flag.FlagSet) {
	console.AbnormalExitIf(flags.Parse(args[1:]), false)
}

func (c *commandParser) format(args []string, flags *flag.FlagSet) {
	c.formatYaml(args, flags)
	c.parse(args, flags)
}
