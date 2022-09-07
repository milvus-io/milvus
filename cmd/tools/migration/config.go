package main

import "flag"

type config struct {
	sourceYaml, targetYaml       string
	sourceVersion, targetVersion string
}

func (c *config) formatSourceYaml(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.sourceYaml, "sourceYaml", "", "set source yaml config")
	if err := flags.Parse(args[2:]); err != nil {
		panic(err)
	}
}

func (c *config) formatSourceVersion(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.sourceVersion, "sourceVersion", "", "set source version")
	if err := flags.Parse(args[2:]); err != nil {
		panic(err)
	}
}

func (c *config) formatTargetYaml(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.targetYaml, "targetYaml", "", "set target yaml config")
	if err := flags.Parse(args[2:]); err != nil {
		panic(err)
	}
}

func (c *config) formatTargetVersion(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.targetVersion, "targetVersion", "", "set target version")
	if err := flags.Parse(args[2:]); err != nil {
		panic(err)
	}
}

func (c *config) format(args []string, flags *flag.FlagSet) {
	c.formatSourceYaml(args, flags)
	c.formatSourceVersion(args, flags)
	c.formatTargetYaml(args, flags)
	c.formatTargetVersion(args, flags)
}
