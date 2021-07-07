package parse_re2

import (
	"regexp"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ introduction
It parses string from the event field using re2 expression with named subgroups and merges the result with the event root.
}*/
type Plugin struct {
	config *Config

	re *regexp.Regexp
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The event field to decode. Must be a string.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"` //*
	Field_ []string

	//> @3@4@5@6
	//>
	//> Re2 expression to use for parsing.
	Re2 string `json:"prefix" default:"" required:"true"`

	//> @3@4@5@6
	//>
	//> A prefix to add to decoded object keys.
	Prefix string `json:"prefix" default:""` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "parse_re2",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	p.re = regexp.MustCompile(p.config.Re2)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	sm := p.re.FindSubmatch(jsonNode.AsBytes())

	jsonNode.Suicide()

	root := insaneJSON.Spawn()

	fields := p.re.SubexpNames()
	for i := 1; i < len(fields); i++ {
		if len(fields[i]) != 0 {
			root.AddFieldNoAlloc(root, fields[i]).MutateToBytesCopy(root, sm[i])
		}
	}

	event.Root.MergeWith(root.Node)

	insaneJSON.Release(root)

	return pipeline.ActionPass
}
