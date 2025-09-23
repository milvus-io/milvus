// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate go run main.go
//go:generate gofumpt -w ../reflect_info.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/samber/lo"
)

const (
	messagePackage  = "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	reflectInfoFile = "reflect_info.json"
	reflectInfoGo   = "../reflect_info.go"
)

type MessageSpecializedType struct {
	HeaderType string `json:"HeaderType"`
	BodyType   string `json:"BodyType"`
}

type MessageTypeWithVersion struct {
	MessageType string `json:"MessageType"`
	Version     int64  `json:"Version"`
}

// JSONMessageSpecializedType represents the JSON format for specialized types
type JSONMessageSpecializedType struct {
	HeaderType string `json:"HeaderType"`
	BodyType   string `json:"BodyType"`
}

// JSONMessageTypeWithVersion represents the JSON format for message with version
type JSONMessageTypeWithVersion struct {
	MessageType string `json:"MessageType"`
	Version     int    `json:"Version"`
}

// JSONMessageReflectInfo represents the JSON format for message reflect info
type JSONMessageReflectInfo struct {
	MessageSpecializedType JSONMessageSpecializedType `json:"MessageSpecializedType"`
	MessageTypeWithVersion JSONMessageTypeWithVersion `json:"MessageTypeWithVersion"`
	NoUtilFunctions        bool                       `json:"NoUtilFunctions"`
}

// JSONConfig represents the JSON configuration file format
type JSONConfig struct {
	Packages                map[string]string        `json:"packages"`
	Consts                  map[string]string        `json:"consts"`
	ExtraExportTypes        []string                 `json:"extraExportTypes"`
	MessageReflectInfoTable []JSONMessageReflectInfo `json:"messageReflectInfoTable"`
}

// Generator is the generator for message reflect info
type Generator struct {
	Config      JSONConfig
	File        *jen.File
	ExportTypes map[string]string
}

// NewGenerator creates a new generator
func NewGenerator() *Generator {
	return &Generator{
		File:        jen.NewFilePathName(messagePackage, "message"),
		ExportTypes: make(map[string]string),
	}
}

// parseTypeString parses a type string like "msgpb.TimeTickMsg" into package and type name
func parseTypeString(typeStr string) (pkg, typeName string) {
	parts := strings.Split(typeStr, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

// getMessageTypeName extracts the message type name from the string
func getMessageTypeName(messageTypeStr string) string {
	if strings.HasPrefix(messageTypeStr, "MessageType") {
		return messageTypeStr[11:] // Remove "MessageType" prefix
	}
	return messageTypeStr
}

// getVersionSuffix converts Version to string suffix
func getVersionSuffix(v int) string {
	return fmt.Sprintf("V%d", v)
}

// getMessageTypeWithVersionId returns the id for the message type with version
func getMessageTypeWithVersionId(info JSONMessageReflectInfo) string {
	msgTypeName := getMessageTypeName(info.MessageTypeWithVersion.MessageType)
	versionSuffix := getVersionSuffix(info.MessageTypeWithVersion.Version)
	return fmt.Sprintf("MessageType%s%s", msgTypeName, versionSuffix)
}

// getSpecializedMessageTypeId returns the type id for the specialized message
func getSpecializedMessageTypeId(info JSONMessageReflectInfo) string {
	msgTypeName := getMessageTypeName(info.MessageTypeWithVersion.MessageType)
	versionSuffix := getVersionSuffix(info.MessageTypeWithVersion.Version)
	return fmt.Sprintf("SpecializedType%s%s", msgTypeName, versionSuffix)
}

func (g *Generator) getPackagePath(pkg string) string {
	if importPath, exists := g.Config.Packages[pkg]; exists {
		return importPath
	}
	panic(fmt.Sprintf("package %s not found", pkg))
}

// createQualifiedType creates a qualified type reference for jennifer
func (g *Generator) createQualifiedType(typeStr string) *jen.Statement {
	pkg, typeName := parseTypeString(typeStr)
	if pkg == "" || g.ExportTypes[typeStr] != "" {
		return jen.Op("*").Id(g.ExportTypes[typeStr])
	}
	return jen.Op("*").Qual(g.getPackagePath(pkg), typeName)
}

// generateHelperFunctions generates all helper functions for a single messageReflectInfo
func (g *Generator) generateHelperFunctions(info JSONMessageReflectInfo) {
	f := g.File

	msgTypeName := getMessageTypeName(info.MessageTypeWithVersion.MessageType)
	versionSuffix := getVersionSuffix(info.MessageTypeWithVersion.Version)

	// Create qualified type references
	headerType := g.createQualifiedType(info.MessageSpecializedType.HeaderType)
	bodyType := g.createQualifiedType(info.MessageSpecializedType.BodyType)

	baseName := msgTypeName + "Message" + versionSuffix

	// Type aliases
	f.Comment(fmt.Sprintf("// Type aliases for %s", baseName))
	f.Type().Id("Mutable"+baseName).Op("=").Qual(messagePackage, "specializedMutableMessage").Types(headerType, bodyType)
	f.Type().Id("Immutable"+baseName).Op("=").Qual(messagePackage, "SpecializedImmutableMessage").Types(headerType, bodyType)
	f.Type().Id("Broadcast"+baseName).Op("=").Qual(messagePackage, "SpecializedBroadcastMessage").Types(headerType, bodyType)
	f.Type().Id("BroadcastResult"+baseName).Op("=").Qual(messagePackage, "BroadcastResult").Types(headerType, bodyType)
	f.Line()

	// MessageTypeWithVersion constant
	f.Comment(fmt.Sprintf("// MessageTypeWithVersion for %s", baseName))
	f.Var().Id(getMessageTypeWithVersionId(info)).Op("=").Qual(messagePackage, "MessageTypeWithVersion").Values(jen.Dict{
		jen.Id("MessageType"): jen.Qual(messagePackage, info.MessageTypeWithVersion.MessageType),
		jen.Id("Version"):     jen.Id("Version" + versionSuffix),
	})
	// MessageSpecializedType constant
	f.Comment(fmt.Sprintf("// MessageSpecializedType for %s", baseName))
	f.Var().Id(getSpecializedMessageTypeId(info)).Op("=").Qual(messagePackage, "MessageSpecializedType").Values(jen.Dict{
		jen.Id("HeaderType"): jen.Qual("reflect", "TypeOf").Call(jen.Parens(headerType).Parens(jen.Nil())),
		jen.Id("BodyType"):   jen.Qual("reflect", "TypeOf").Call(jen.Parens(bodyType).Parens(jen.Nil())),
	})

	if !info.NoUtilFunctions {
		// AsMutable function
		f.Comment(fmt.Sprintf("// AsMutable%s converts a BasicMessage to Mutable%s", baseName, baseName))
		f.Var().Id("AsMutable"+baseName).Op("=").Qual(messagePackage, "asSpecializedMutableMessage").Types(headerType, bodyType)
		// MustAsMutable function
		f.Comment(fmt.Sprintf("// MustAsMutable%s converts a BasicMessage to Mutable%s, panics on error", baseName, baseName))
		f.Var().Id("MustAsMutable"+baseName).Op("=").Qual(messagePackage, "mustAsSpecializedMutableMessage").Types(headerType, bodyType)
		// AsImmutable function
		f.Comment(fmt.Sprintf("// AsImmutable%s converts an ImmutableMessage to Immutable%s", baseName, baseName))
		f.Var().Id("AsImmutable"+baseName).Op("=").Qual(messagePackage, "asSpecializedImmutableMessage").Types(headerType, bodyType)
		// MustAsImmutable function
		f.Comment(fmt.Sprintf("// MustAsImmutable%s converts an ImmutableMessage to Immutable%s, panics on error", baseName, baseName))
		f.Var().Id("MustAsImmutable"+baseName).Op("=").Qual(messagePackage, "MustAsSpecializedImmutableMessage").Types(headerType, bodyType)
		// AsBroadcast function
		f.Comment(fmt.Sprintf("// AsBroadcast%s converts a BasicMessage to Broadcast%s", baseName, baseName))
		f.Var().Id("AsBroadcast"+baseName).Op("=").Qual(messagePackage, "asSpecializedBroadcastMessage").Types(headerType, bodyType)
		// MustAsBroadcast function
		f.Comment(fmt.Sprintf("// MustAsBroadcast%s converts a BasicMessage to Broadcast%s, panics on error", baseName, baseName))
		f.Var().Id("MustAsBroadcast"+baseName).Op("=").Qual(messagePackage, "MustAsSpecializedBroadcastMessage").Types(headerType, bodyType)
		f.Line()

		// NewBuilder function
		f.Comment(fmt.Sprintf("// New%sMessageBuilder%s creates a new message builder for %s", msgTypeName, versionSuffix, baseName))
		f.Var().Id("New"+msgTypeName+"MessageBuilder"+versionSuffix).Op("=").Qual(messagePackage, "newMutableMessageBuilder").Types(headerType, bodyType)
		f.Line()
	}
}

// GenerateCodeFromJSON generates Go code from JSON configuration
func (g *Generator) GenerateCodeFromJSON(jsonData []byte) (string, error) {
	if err := json.Unmarshal(jsonData, &g.Config); err != nil {
		return "", fmt.Errorf("failed to parse JSON: %w", err)
	}

	g.File.HeaderComment("// Code generated by message-codegen. DO NOT EDIT.")

	// Generate consts
	g.generateConsts()

	// Export types
	g.exportTypes()

	// Generate functions for each messageReflectInfo
	for _, info := range g.Config.MessageReflectInfoTable {
		g.generateHelperFunctions(info)
	}

	// Generate tables
	g.generateTables()

	// Generate export AsImmutableTxnMessage
	return g.File.GoString(), nil
}

func (g *Generator) generateConsts() {
	g.File.Comment("// Export consts")
	keys := lo.Keys(g.Config.Consts)
	sort.Strings(keys)
	for _, constName := range keys {
		constValue := g.Config.Consts[constName]
		g.File.Const().Id(constName).Op("=").Lit(constValue)
	}
}

// exportTypes exports the types for the message reflect info
func (g *Generator) exportTypes() {
	// Generate message types
	g.File.Comment("// Export message types")
	g.File.Const().Id("MessageTypeUnknown").
		Qual(messagePackage, "MessageType").Op("=").Qual(messagePackage, "MessageType").
		Call(jen.Qual(g.getPackagePath("messagespb"), "MessageType_Unknown"))
	dedupMsgTypeName := make(map[string]struct{})
	for _, info := range g.Config.MessageReflectInfoTable {
		msgTypeName := getMessageTypeName(info.MessageTypeWithVersion.MessageType)
		if _, ok := dedupMsgTypeName[msgTypeName]; ok {
			continue
		}
		dedupMsgTypeName[msgTypeName] = struct{}{}
		g.File.Const().Id(info.MessageTypeWithVersion.MessageType).
			Qual(messagePackage, "MessageType").Op("=").Qual(messagePackage, "MessageType").
			Call(jen.Qual(g.getPackagePath("messagespb"), "MessageType_"+msgTypeName))
	}

	// Generate extra export types
	g.File.Comment("// Export extra message type")
	for _, extraExportType := range g.Config.ExtraExportTypes {
		pkg, typeName := parseTypeString(extraExportType)
		g.File.Type().Id(typeName).Op("=").Qual(g.getPackagePath(pkg), typeName)
		g.ExportTypes[extraExportType] = typeName
	}
	g.File.Line()

	// Export message header and body types
	g.File.Comment("// Export message header and body types")
	dedupMsgHeaderTypeName := make(map[string]struct{})
	dedupMsgBodyTypeName := make(map[string]struct{})
	for _, info := range g.Config.MessageReflectInfoTable {
		headerPkg, headerName := parseTypeString(info.MessageSpecializedType.HeaderType)
		bodyPkg, bodyName := parseTypeString(info.MessageSpecializedType.BodyType)
		if _, ok := dedupMsgHeaderTypeName[headerName]; !ok {
			g.File.Type().Id(headerName).Op("=").Qual(g.getPackagePath(headerPkg), headerName)
			g.ExportTypes[info.MessageSpecializedType.HeaderType] = headerName
			dedupMsgHeaderTypeName[headerName] = struct{}{}
		}
		if _, ok := dedupMsgBodyTypeName[bodyName]; !ok {
			g.File.Type().Id(bodyName).Op("=").Qual(g.getPackagePath(bodyPkg), bodyName)
			g.ExportTypes[info.MessageSpecializedType.BodyType] = bodyName
			dedupMsgBodyTypeName[bodyName] = struct{}{}
		}
	}
	g.File.Line()
}

// generateTables generates the tables for the message reflect info
func (g *Generator) generateTables() {
	// Generate messageTypeMap
	g.File.Comment("// messageTypeMap make the contriants that one header type can only be used for one message type.")
	g.File.Var().Id("messageTypeMap").Op("=").Map(jen.Qual("reflect", "Type")).Qual(messagePackage, "MessageType").Values(
		jen.DictFunc(func(d jen.Dict) {
			for _, info := range g.Config.MessageReflectInfoTable {
				headerPkg, headerName := parseTypeString(info.MessageSpecializedType.HeaderType)
				d[jen.Qual("reflect", "TypeOf").Call(jen.Op("&").Qual(g.getPackagePath(headerPkg), headerName).Block())] = jen.Qual(messagePackage, info.MessageTypeWithVersion.MessageType)
			}
		}),
	)
	g.File.Line()

	// Generate MessageTypeWithVersion and MessageSpecializedType
	g.File.Comment("// MessageTypeWithVersion identifies a message type and version")
	g.File.Type().Id("MessageTypeWithVersion").Struct(
		jen.Id("MessageType").Qual(messagePackage, "MessageType"),
		jen.Id("Version").Qual(messagePackage, "Version"),
	)
	g.File.Line()

	// String returns a string representation of MessageTypeWithVersion
	g.File.Func().Params(
		jen.Id("m").Id("MessageTypeWithVersion"),
	).Id("String").Params().String().Block(
		jen.Return(jen.Qual("fmt", "Sprintf").Call(
			jen.Lit("%s@v%d"),
			jen.Id("m").Dot("MessageType").Dot("String").Call(),
			jen.Id("m").Dot("Version"),
		)),
	)
	g.File.Line()

	g.File.Comment("// MessageSpecializedType contains reflection types for message headers and bodies")
	g.File.Type().Id("MessageSpecializedType").Struct(
		jen.Id("HeaderType").Qual("reflect", "Type"),
		jen.Id("BodyType").Qual("reflect", "Type"),
	)
	g.File.Line()

	// Generate mapping table
	g.File.Comment("// messageTypeVersionSpecializedMap maps MessageTypeWithVersion to MessageSpecializedType")
	g.File.Var().Id("messageTypeVersionSpecializedMap").Op("=").Map(jen.Id("MessageTypeWithVersion")).Id("MessageSpecializedType").Values(
		jen.DictFunc(func(d jen.Dict) {
			for _, info := range g.Config.MessageReflectInfoTable {
				key := jen.Id(getMessageTypeWithVersionId(info))
				d[key] = jen.Id(getSpecializedMessageTypeId(info))
			}
		}),
	)
	g.File.Line()

	// Generate reverse mapping table
	g.File.Comment("// messageSpecializedTypeVersionMap maps MessageSpecializedType to MessageTypeWithVersion")
	g.File.Var().Id("messageSpecializedTypeVersionMap").Op("=").Map(jen.Id("MessageSpecializedType")).Id("MessageTypeWithVersion").Values(
		jen.DictFunc(func(d jen.Dict) {
			for _, info := range g.Config.MessageReflectInfoTable {
				key := jen.Id(getSpecializedMessageTypeId(info))
				d[key] = jen.Id(getMessageTypeWithVersionId(info))
			}
		}),
	)
	g.File.Line()
}

// codegen generates the code from the JSON file
func codegen() (string, error) {
	// Read JSON file
	jsonData, err := os.ReadFile(reflectInfoFile)
	if err != nil {
		return "", err
	}

	// Generate code
	g := NewGenerator()
	code, err := g.GenerateCodeFromJSON(jsonData)
	if err != nil {
		return "", err
	}
	return code, nil
}

func main() {
	code, err := codegen()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating code: %v\n", err)
		os.Exit(1)
	}

	err = os.WriteFile(reflectInfoGo, []byte(code), 0o644) // #nosec G306
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing to file %s: %v\n", reflectInfoGo, err)
		os.Exit(1)
	}
	fmt.Printf("Generated code written to %s\n", reflectInfoGo)
}
