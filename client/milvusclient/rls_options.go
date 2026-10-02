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

package milvusclient

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// CreateRowPolicyOption builds a CreateRowPolicy request.
type CreateRowPolicyOption interface {
	Request() *milvuspb.CreateRowPolicyRequest
}

type createRowPolicyOption struct {
	dbName         string
	collectionName string
	policyName     string
	policyType     milvuspb.RowPolicyType
	actions        []milvuspb.RowPolicyAction
	usingExpr      string
	checkExpr      string
	description    string
}

func NewCreateRowPolicyOption(collectionName, policyName string, policyType milvuspb.RowPolicyType, actions ...milvuspb.RowPolicyAction) *createRowPolicyOption {
	return &createRowPolicyOption{
		collectionName: collectionName,
		policyName:     policyName,
		policyType:     policyType,
		actions:        actions,
	}
}

func (opt *createRowPolicyOption) WithDbName(dbName string) *createRowPolicyOption {
	opt.dbName = dbName
	return opt
}

func (opt *createRowPolicyOption) WithUsingExpr(expr string) *createRowPolicyOption {
	opt.usingExpr = expr
	return opt
}

func (opt *createRowPolicyOption) WithCheckExpr(expr string) *createRowPolicyOption {
	opt.checkExpr = expr
	return opt
}

func (opt *createRowPolicyOption) WithDescription(description string) *createRowPolicyOption {
	opt.description = description
	return opt
}

func (opt *createRowPolicyOption) Request() *milvuspb.CreateRowPolicyRequest {
	return &milvuspb.CreateRowPolicyRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		PolicyName:     opt.policyName,
		Actions:        opt.actions,
		UsingExpr:      opt.usingExpr,
		CheckExpr:      opt.checkExpr,
		Description:    opt.description,
		PolicyType:     opt.policyType.Enum(),
	}
}

// UpdateRowPolicyOption builds an UpdateRowPolicy request.
type UpdateRowPolicyOption interface {
	Request() *milvuspb.UpdateRowPolicyRequest
}

type updateRowPolicyOption struct {
	dbName         string
	collectionName string
	policyName     string
	policyType     milvuspb.RowPolicyType
	actions        []milvuspb.RowPolicyAction
	usingExpr      string
	checkExpr      string
	description    string
}

func NewUpdateRowPolicyOption(collectionName, policyName string, policyType milvuspb.RowPolicyType, actions ...milvuspb.RowPolicyAction) *updateRowPolicyOption {
	return &updateRowPolicyOption{
		collectionName: collectionName,
		policyName:     policyName,
		policyType:     policyType,
		actions:        actions,
	}
}

func (opt *updateRowPolicyOption) WithDbName(dbName string) *updateRowPolicyOption {
	opt.dbName = dbName
	return opt
}

func (opt *updateRowPolicyOption) WithUsingExpr(expr string) *updateRowPolicyOption {
	opt.usingExpr = expr
	return opt
}

func (opt *updateRowPolicyOption) WithCheckExpr(expr string) *updateRowPolicyOption {
	opt.checkExpr = expr
	return opt
}

func (opt *updateRowPolicyOption) WithDescription(description string) *updateRowPolicyOption {
	opt.description = description
	return opt
}

func (opt *updateRowPolicyOption) Request() *milvuspb.UpdateRowPolicyRequest {
	return &milvuspb.UpdateRowPolicyRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		PolicyName:     opt.policyName,
		Actions:        opt.actions,
		UsingExpr:      opt.usingExpr,
		CheckExpr:      opt.checkExpr,
		Description:    opt.description,
		PolicyType:     opt.policyType,
	}
}

// DropRowPolicyOption builds a DropRowPolicy request.
type DropRowPolicyOption interface {
	Request() *milvuspb.DropRowPolicyRequest
}

type dropRowPolicyOption struct {
	dbName         string
	collectionName string
	policyName     string
}

func NewDropRowPolicyOption(collectionName, policyName string) *dropRowPolicyOption {
	return &dropRowPolicyOption{collectionName: collectionName, policyName: policyName}
}

func (opt *dropRowPolicyOption) WithDbName(dbName string) *dropRowPolicyOption {
	opt.dbName = dbName
	return opt
}

func (opt *dropRowPolicyOption) Request() *milvuspb.DropRowPolicyRequest {
	return &milvuspb.DropRowPolicyRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		PolicyName:     opt.policyName,
	}
}

// ListRowPoliciesOption builds a ListRowPolicies request.
type ListRowPoliciesOption interface {
	Request() *milvuspb.ListRowPoliciesRequest
}

type listRowPoliciesOption struct {
	dbName         string
	collectionName string
}

func NewListRowPoliciesOption(collectionName string) *listRowPoliciesOption {
	return &listRowPoliciesOption{collectionName: collectionName}
}

func (opt *listRowPoliciesOption) WithDbName(dbName string) *listRowPoliciesOption {
	opt.dbName = dbName
	return opt
}

func (opt *listRowPoliciesOption) Request() *milvuspb.ListRowPoliciesRequest {
	return &milvuspb.ListRowPoliciesRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

// SetRLSPrincipalTagsOption builds a SetRLSPrincipalTags request.
type SetRLSPrincipalTagsOption interface {
	Request() *milvuspb.SetRLSPrincipalTagsRequest
}

type setRLSPrincipalTagsOption struct {
	dbName         string
	collectionName string
	principalName  string
	tags           string
}

func NewSetRLSPrincipalTagsOption(collectionName, principalName, tags string) *setRLSPrincipalTagsOption {
	return &setRLSPrincipalTagsOption{
		collectionName: collectionName,
		principalName:  principalName,
		tags:           tags,
	}
}

func (opt *setRLSPrincipalTagsOption) WithDbName(dbName string) *setRLSPrincipalTagsOption {
	opt.dbName = dbName
	return opt
}

func (opt *setRLSPrincipalTagsOption) Request() *milvuspb.SetRLSPrincipalTagsRequest {
	return &milvuspb.SetRLSPrincipalTagsRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		PrincipalName:  opt.principalName,
		Tags:           opt.tags,
	}
}

// GetRLSPrincipalTagsOption builds a GetRLSPrincipalTags request.
type GetRLSPrincipalTagsOption interface {
	Request() *milvuspb.GetRLSPrincipalTagsRequest
}

type getRLSPrincipalTagsOption struct {
	dbName         string
	collectionName string
	principalName  string
}

func NewGetRLSPrincipalTagsOption(collectionName, principalName string) *getRLSPrincipalTagsOption {
	return &getRLSPrincipalTagsOption{collectionName: collectionName, principalName: principalName}
}

func (opt *getRLSPrincipalTagsOption) WithDbName(dbName string) *getRLSPrincipalTagsOption {
	opt.dbName = dbName
	return opt
}

func (opt *getRLSPrincipalTagsOption) Request() *milvuspb.GetRLSPrincipalTagsRequest {
	return &milvuspb.GetRLSPrincipalTagsRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		PrincipalName:  opt.principalName,
	}
}

// ListRLSPrincipalsOption builds a ListRLSPrincipals request.
type ListRLSPrincipalsOption interface {
	Request() *milvuspb.ListRLSPrincipalsRequest
}

type listRLSPrincipalsOption struct {
	dbName         string
	collectionName string
}

func NewListRLSPrincipalsOption(collectionName string) *listRLSPrincipalsOption {
	return &listRLSPrincipalsOption{collectionName: collectionName}
}

func (opt *listRLSPrincipalsOption) WithDbName(dbName string) *listRLSPrincipalsOption {
	opt.dbName = dbName
	return opt
}

func (opt *listRLSPrincipalsOption) Request() *milvuspb.ListRLSPrincipalsRequest {
	return &milvuspb.ListRLSPrincipalsRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

// DeleteRLSPrincipalTagsOption builds a DeleteRLSPrincipalTags request.
type DeleteRLSPrincipalTagsOption interface {
	Request() *milvuspb.DeleteRLSPrincipalTagsRequest
}

type deleteRLSPrincipalTagsOption struct {
	dbName         string
	collectionName string
	principalName  string
	tagKeys        []string
}

func NewDeleteRLSPrincipalTagsOption(collectionName, principalName string, tagKeys ...string) *deleteRLSPrincipalTagsOption {
	return &deleteRLSPrincipalTagsOption{
		collectionName: collectionName,
		principalName:  principalName,
		tagKeys:        tagKeys,
	}
}

func (opt *deleteRLSPrincipalTagsOption) WithDbName(dbName string) *deleteRLSPrincipalTagsOption {
	opt.dbName = dbName
	return opt
}

func (opt *deleteRLSPrincipalTagsOption) Request() *milvuspb.DeleteRLSPrincipalTagsRequest {
	return &milvuspb.DeleteRLSPrincipalTagsRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		PrincipalName:  opt.principalName,
		TagKeys:        opt.tagKeys,
	}
}

var (
	_ CreateRowPolicyOption        = (*createRowPolicyOption)(nil)
	_ UpdateRowPolicyOption        = (*updateRowPolicyOption)(nil)
	_ DropRowPolicyOption          = (*dropRowPolicyOption)(nil)
	_ ListRowPoliciesOption        = (*listRowPoliciesOption)(nil)
	_ SetRLSPrincipalTagsOption    = (*setRLSPrincipalTagsOption)(nil)
	_ GetRLSPrincipalTagsOption    = (*getRLSPrincipalTagsOption)(nil)
	_ ListRLSPrincipalsOption      = (*listRLSPrincipalsOption)(nil)
	_ DeleteRLSPrincipalTagsOption = (*deleteRLSPrincipalTagsOption)(nil)
)
