package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCatalog_AlterCollectionAndDeleteGrants(t *testing.T) {
	oldColl := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db"}
	newColl := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db"}
	ts := typeutil.Timestamp(100)

	mockey.PatchConvey("success executes alter then grant deletion", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, alterType metastore.AlterType, _ typeutil.Timestamp, fieldModify bool) error {
			assert.Equal(t, metastore.MODIFY, alterType)
			assert.False(t, fieldModify)
			calls = append(calls, "AlterCollection")
			return nil
		}).Build()
		mockey.Mock((*Catalog).DeleteGrantByCollectionName).To(func(_ *Catalog, _ context.Context, tenant, dbName, collectionName string) error {
			assert.Equal(t, "tenant1", tenant)
			assert.Equal(t, "db", dbName)
			assert.Equal(t, "coll", collectionName)
			calls = append(calls, "DeleteGrantByCollectionName")
			return nil
		}).Build()

		err := kc.AlterCollectionAndDeleteGrants(context.TODO(), oldColl, newColl, ts, "tenant1", "db", "coll")
		assert.NoError(t, err)
		assert.Equal(t, []string{"AlterCollection", "DeleteGrantByCollectionName"}, calls)
	})

	mockey.PatchConvey("alter failure aborts before grant deletion", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			calls = append(calls, "AlterCollection")
			return errors.New("alter failed")
		}).Build()
		mockey.Mock((*Catalog).DeleteGrantByCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _ string) error {
			calls = append(calls, "DeleteGrantByCollectionName")
			return nil
		}).Build()

		err := kc.AlterCollectionAndDeleteGrants(context.TODO(), oldColl, newColl, ts, "tenant1", "db", "coll")
		assert.Error(t, err)
		assert.Equal(t, []string{"AlterCollection"}, calls)
	})

	mockey.PatchConvey("grant deletion failure is best-effort", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).DeleteGrantByCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _ string) error {
			return errors.New("grant deletion failed")
		}).Build()

		err := kc.AlterCollectionAndDeleteGrants(context.TODO(), oldColl, newColl, ts, "tenant1", "db", "coll")
		assert.NoError(t, err)
	})
}

func TestCatalog_DropCollectionAndDeleteGrants(t *testing.T) {
	coll := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db"}
	ts := typeutil.Timestamp(100)

	mockey.PatchConvey("success executes drop then grant deletion", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropCollection).To(func(_ *Catalog, _ context.Context, _ *model.Collection, _ typeutil.Timestamp) error {
			calls = append(calls, "DropCollection")
			return nil
		}).Build()
		mockey.Mock((*Catalog).DeleteGrantByCollectionName).To(func(_ *Catalog, _ context.Context, tenant, dbName, collectionName string) error {
			assert.Equal(t, "tenant1", tenant)
			assert.Equal(t, "db", dbName)
			assert.Equal(t, "coll", collectionName)
			calls = append(calls, "DeleteGrantByCollectionName")
			return nil
		}).Build()

		err := kc.DropCollectionAndDeleteGrants(context.TODO(), coll, ts, "tenant1", "db", "coll")
		assert.NoError(t, err)
		assert.Equal(t, []string{"DropCollection", "DeleteGrantByCollectionName"}, calls)
	})

	mockey.PatchConvey("drop failure aborts before grant deletion", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropCollection).To(func(_ *Catalog, _ context.Context, _ *model.Collection, _ typeutil.Timestamp) error {
			calls = append(calls, "DropCollection")
			return errors.New("drop failed")
		}).Build()
		mockey.Mock((*Catalog).DeleteGrantByCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _ string) error {
			calls = append(calls, "DeleteGrantByCollectionName")
			return nil
		}).Build()

		err := kc.DropCollectionAndDeleteGrants(context.TODO(), coll, ts, "tenant1", "db", "coll")
		assert.Error(t, err)
		assert.Equal(t, []string{"DropCollection"}, calls)
	})

	mockey.PatchConvey("grant deletion failure is best-effort", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).DropCollection).To(func(_ *Catalog, _ context.Context, _ *model.Collection, _ typeutil.Timestamp) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).DeleteGrantByCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _ string) error {
			return errors.New("grant deletion failed")
		}).Build()

		err := kc.DropCollectionAndDeleteGrants(context.TODO(), coll, ts, "tenant1", "db", "coll")
		assert.NoError(t, err)
	})
}

func TestCatalog_AlterCollectionAndMigrateGrants(t *testing.T) {
	ts := typeutil.Timestamp(100)

	mockey.PatchConvey("rename in place migrates grants after alter", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, alterType metastore.AlterType, _ typeutil.Timestamp, fieldModify bool) error {
			assert.Equal(t, metastore.MODIFY, alterType)
			assert.True(t, fieldModify)
			calls = append(calls, "AlterCollection")
			return nil
		}).Build()
		mockey.Mock((*Catalog).AlterCollectionDB).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ typeutil.Timestamp) error {
			calls = append(calls, "AlterCollectionDB")
			return nil
		}).Build()
		mockey.Mock((*Catalog).MigrateGrantCollectionName).To(func(_ *Catalog, _ context.Context, tenant, oldDBName, oldName, newDBName, newName string) error {
			assert.Equal(t, "tenant1", tenant)
			assert.Equal(t, "db", oldDBName)
			assert.Equal(t, "old", oldName)
			assert.Equal(t, "db", newDBName)
			assert.Equal(t, "new", newName)
			calls = append(calls, "MigrateGrantCollectionName")
			return nil
		}).Build()

		oldColl := &model.Collection{CollectionID: 1, Name: "old", DBName: "db"}
		newColl := &model.Collection{CollectionID: 1, Name: "new", DBName: "db"}
		err := kc.AlterCollectionAndMigrateGrants(context.TODO(), oldColl, newColl, ts, true, false, "tenant1")
		assert.NoError(t, err)
		assert.Equal(t, []string{"AlterCollection", "MigrateGrantCollectionName"}, calls)
	})

	mockey.PatchConvey("db change routes to AlterCollectionDB", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			calls = append(calls, "AlterCollection")
			return nil
		}).Build()
		mockey.Mock((*Catalog).AlterCollectionDB).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ typeutil.Timestamp) error {
			calls = append(calls, "AlterCollectionDB")
			return nil
		}).Build()
		mockey.Mock((*Catalog).MigrateGrantCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _, _, _ string) error {
			calls = append(calls, "MigrateGrantCollectionName")
			return nil
		}).Build()

		oldColl := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db1"}
		newColl := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db2"}
		err := kc.AlterCollectionAndMigrateGrants(context.TODO(), oldColl, newColl, ts, false, true, "tenant1")
		assert.NoError(t, err)
		assert.Equal(t, []string{"AlterCollectionDB", "MigrateGrantCollectionName"}, calls)
	})

	mockey.PatchConvey("no rename skips grant migration", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			calls = append(calls, "AlterCollection")
			return nil
		}).Build()
		mockey.Mock((*Catalog).MigrateGrantCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _, _, _ string) error {
			calls = append(calls, "MigrateGrantCollectionName")
			return nil
		}).Build()

		oldColl := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db"}
		newColl := &model.Collection{CollectionID: 1, Name: "coll", DBName: "db"}
		err := kc.AlterCollectionAndMigrateGrants(context.TODO(), oldColl, newColl, ts, false, false, "tenant1")
		assert.NoError(t, err)
		assert.Equal(t, []string{"AlterCollection"}, calls)
	})

	mockey.PatchConvey("alter failure aborts before grant migration", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			calls = append(calls, "AlterCollection")
			return errors.New("alter failed")
		}).Build()
		mockey.Mock((*Catalog).MigrateGrantCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _, _, _ string) error {
			calls = append(calls, "MigrateGrantCollectionName")
			return nil
		}).Build()

		oldColl := &model.Collection{CollectionID: 1, Name: "old", DBName: "db"}
		newColl := &model.Collection{CollectionID: 1, Name: "new", DBName: "db"}
		err := kc.AlterCollectionAndMigrateGrants(context.TODO(), oldColl, newColl, ts, false, false, "tenant1")
		assert.Error(t, err)
		assert.Equal(t, []string{"AlterCollection"}, calls)
	})

	mockey.PatchConvey("grant migration failure is best-effort", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).AlterCollection).To(func(_ *Catalog, _ context.Context, _, _ *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).MigrateGrantCollectionName).To(func(_ *Catalog, _ context.Context, _, _, _, _, _ string) error {
			return errors.New("migration failed")
		}).Build()

		oldColl := &model.Collection{CollectionID: 1, Name: "old", DBName: "db"}
		newColl := &model.Collection{CollectionID: 1, Name: "new", DBName: "db"}
		err := kc.AlterCollectionAndMigrateGrants(context.TODO(), oldColl, newColl, ts, false, false, "tenant1")
		assert.NoError(t, err)
	})
}

func TestCatalog_DropRoleAndGrants(t *testing.T) {
	mockey.PatchConvey("success executes drop role then delete grant", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropRole).To(func(_ *Catalog, _ context.Context, tenant, roleName string) error {
			assert.Equal(t, "tenant1", tenant)
			assert.Equal(t, "role1", roleName)
			calls = append(calls, "DropRole")
			return nil
		}).Build()
		mockey.Mock((*Catalog).DeleteGrant).To(func(_ *Catalog, _ context.Context, tenant string, role *milvuspb.RoleEntity) error {
			assert.Equal(t, "tenant1", tenant)
			assert.Equal(t, "role1", role.GetName())
			calls = append(calls, "DeleteGrant")
			return nil
		}).Build()

		err := kc.DropRoleAndGrants(context.TODO(), "tenant1", "role1")
		assert.NoError(t, err)
		assert.Equal(t, []string{"DropRole", "DeleteGrant"}, calls)
	})

	mockey.PatchConvey("drop role failure aborts before delete grant", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropRole).To(func(_ *Catalog, _ context.Context, _, _ string) error {
			calls = append(calls, "DropRole")
			return errors.New("drop role failed")
		}).Build()
		mockey.Mock((*Catalog).DeleteGrant).To(func(_ *Catalog, _ context.Context, _ string, _ *milvuspb.RoleEntity) error {
			calls = append(calls, "DeleteGrant")
			return nil
		}).Build()

		err := kc.DropRoleAndGrants(context.TODO(), "tenant1", "role1")
		assert.Error(t, err)
		assert.Equal(t, []string{"DropRole"}, calls)
	})

	mockey.PatchConvey("delete grant failure is returned (fail-hard)", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).DropRole).To(func(_ *Catalog, _ context.Context, _, _ string) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).DeleteGrant).To(func(_ *Catalog, _ context.Context, _ string, _ *milvuspb.RoleEntity) error {
			return errors.New("delete grant failed")
		}).Build()

		err := kc.DropRoleAndGrants(context.TODO(), "tenant1", "role1")
		assert.Error(t, err)
	})
}
