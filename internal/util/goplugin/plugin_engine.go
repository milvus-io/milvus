package goplugin

import (
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

type PluginEngine struct {
	i *interp.Interpreter
}

// TODO we don't find a way to import milvus related logic
func (p *PluginEngine) Init(path string) error {
	p.i = interp.New(interp.Options{})

	if err := p.i.Use(stdlib.Symbols); err != nil {
		return err
	}

	_, err := p.i.EvalPath(path)
	if err != nil {
		return err
	}

	return nil
}

// get the go file path and function name, return function reflection
func (p PluginEngine) GetFunc(funcName string) (any, error) {
	v, err := p.i.Eval(funcName)
	if err != nil {
		return nil, err
	}
	return v.Interface(), nil
}
