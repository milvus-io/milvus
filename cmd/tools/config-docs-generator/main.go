package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var (
	inputFile  = "configs/milvus.yaml"
	outputPath = os.Getenv("PWD")
)

func main() {
	flag.StringVar(&inputFile, "i", inputFile, "input file")
	flag.StringVar(&outputPath, "o", outputPath, "output path")
	flag.Parse()
	log.Printf("start generating input[%s], output[%s]", inputFile, outputPath)
	err := run()
	if err != nil {
		log.Fatal(err)
	}
	log.Print("generate successed")
}

func run() error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		return errors.Wrap(err, "read config file")
	}
	var target yaml.Node
	err = yaml.Unmarshal(data, &target)
	if err != nil {
		return errors.Wrap(err, "unmarshal config file")
	}
	err = generateDocs(target.Content[0])
	return err
}

func generateDocs(root *yaml.Node) error {
	sections := parseSections(root)
	err := generateFiles(sections)
	if err != nil {
		return err
	}
	return nil
}

func parseSections(root *yaml.Node) []Section {
	var printed bool
	var sections []Section
	for i := 0; i < len(root.Content); i++ {
		section := Section{
			Name:        root.Content[i].Value,
			Description: getDescriptionFromNode(root.Content[i]),
		}
		i++
		section.Fields = parseMapFields(section.Name, root.Content[i])
		if !printed && len(section.Fields) > 0 {
			printed = true
		}

		sections = append(sections, section)
	}
	return sections
}

// head commet + line comment, remove # prefix, then join with '\n'
func getDescriptionFromNode(node *yaml.Node) []string {
	var retLines []string
	if node.HeadComment != "" {
		retLines = append(retLines, strings.Split(node.HeadComment, "\n")...)
	}
	if node.LineComment != "" {
		retLines = append(retLines, strings.Split(node.LineComment, "\n")...)
	}
	for i := 0; i < len(retLines); i++ {
		retLines[i] = strings.ReplaceAll(strings.TrimPrefix(retLines[i], "# "), "\n# ", "\n")
	}
	return retLines
}

// yaml tags copied from `yaml/resolve.go`
const (
	nullTag      = "!!null"
	boolTag      = "!!bool"
	strTag       = "!!str"
	intTag       = "!!int"
	floatTag     = "!!float"
	timestampTag = "!!timestamp"
	seqTag       = "!!seq"
	mapTag       = "!!map"
	binaryTag    = "!!binary"
	mergeTag     = "!!merge"
)

// parseMapFields
func parseMapFields(prefix string, sectionNode *yaml.Node) []Field {
	// recursively parses into the node till it reaches the leaf node
	var fields []Field
	for i := 0; i < len(sectionNode.Content); i += 2 {
		subNode := sectionNode.Content[i]
		subNodeData := sectionNode.Content[i+1]
		if len(prefix) >= 4 && prefix[0:4] == "etcd" {
			log.Print(subNode.Value, subNodeData.Kind, subNodeData.LineComment)
		}
		switch subNodeData.Kind {
		case yaml.MappingNode:
			fields = append(fields, parseMapFields(prefix+"."+subNode.Value, subNodeData)...)
		// case yaml.SequenceNode:
		// TODO:
		// fields = append(fields, parseMapFields(prefix+"."+subNode.Value, subNode)...)
		default:
			// assume k v pair
			fields = append(fields, Field{
				Name:         prefix + "." + subNode.Value,
				Description:  append(getDescriptionFromNode(subNode), getDescriptionFromNode(subNodeData)...),
				DefaultValue: parseDefaultValue(subNodeData),
			})
		}
	}
	return fields
}

func parseDefaultValue(node *yaml.Node) string {
	// parse node of scarlar or sequence
	switch node.Tag {
	case intTag, floatTag, strTag, boolTag, nullTag, timestampTag, binaryTag:
		return node.Value
	case seqTag:
		// parse sequence
		var retArray []string
		for _, v := range node.Content {
			// we assume that the sequence is a list of scalars
			retArray = append(retArray, parseDefaultValue(v))
		}
		return strings.Join(retArray, ", ")
	default:
		return "<todo>"
	}
}

func generateFiles(secs []Section) error {
	const head = `---
id: system_configuration.md
related_key: configure
group: system_configuration.md
summary: Learn about the system configuration of Milvus.
---

# Milvus System Configurations Checklist

This topic introduces the general sections of the system configurations in Milvus.

Milvus maintains a considerable number of parameters that configure the system. Each configuration has a default value, which can be used directly. You can modify these parameters flexibly so that Milvus can better serve your application. See [Configure Milvus](configure-docker.md) for more information.

<div class="alert note">
In current release, all parameters take effect only after being configured at the startup of Milvus.
</div>

## Sections

For the convenience of maintenance, Milvus classifies its configurations into %s sections based on its components, dependencies, and general usage.

`
	const fileName = "system_configuration.md"
	fileContent := head
	for _, sec := range secs {
		fileContent += sec.systemConfiguratinContent()
		sectionFileContent := sec.sectionPageContent()
		os.WriteFile(filepath.Join(outputPath, sec.fileName()), []byte(sectionFileContent), 0o644)
	}
	err := os.WriteFile(filepath.Join(outputPath, fileName), []byte(fileContent), 0o644)
	return errors.Wrapf(err, "writefile %s", fileName)
}

type Section struct {
	Name        string
	Description []string
	Fields      []Field
}

func (s Section) systemConfiguratinContent() string {
	return fmt.Sprintf("### `%s`"+mdNextLine+
		"%s"+mdNextLine+
		"See [%s-related Configurations](%s) for detailed description for each parameter under this section."+mdNextLine,
		s.Name, s.descriptionContent(), s.Name, s.fileName())
}

func (s Section) fileName() string {
	return fmt.Sprintf("configure_%s.md", strings.ToLower(s.Name))
}

const mdNextLine = "\n\n"

func (s Section) descriptionContent() string {
	return strings.Join(s.Description, mdNextLine)
}

const sectionFileHeadTemplate = `---
id: %s
related_key: configure
group: system_configuration.md
summary: Learn how to configure %s for Milvus.
---

`

func (s Section) sectionPageContent() string {
	ret := fmt.Sprintf(sectionFileHeadTemplate, s.fileName(), s.Name)
	ret += fmt.Sprintf("# %s-related Configurations"+mdNextLine, s.Name)
	ret += s.descriptionContent() + mdNextLine
	for _, field := range s.Fields {
		if len(field.Description) == 0 || field.Description[0] == "" {
			continue
		}
		ret += field.sectionPageContent() + mdNextLine
	}

	return ret
}

type Field struct {
	Name         string
	Description  []string
	DefaultValue string
}

const fieldTableTemplate = `<table id="%s">
  <thead>
    <tr>
      <th class="width80">Description</th>
      <th class="width20">Default Value</th> 
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>%s</td>
      <td>%s</td>
    </tr>
  </tbody>
</table>
`

func (f Field) sectionPageContent() string {
	ret := fmt.Sprintf("## `%s`", f.Name) + mdNextLine
	desp := f.descriptionContent()
	ret += fmt.Sprintf(fieldTableTemplate, f.Name, desp, f.DefaultValue)
	return ret
}

func (f Field) descriptionContent() string {
	var ret string
	lines := len(f.Description)
	if lines > 1 {
		for _, descLine := range f.Description {
			ret += fmt.Sprintf("\n        <li>%s</li>      ", descLine)
		}
	} else {
		ret = fmt.Sprintf("        %s      ", f.Description[0])
	}

	return ret
}
