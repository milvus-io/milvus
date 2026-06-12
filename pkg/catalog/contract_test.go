package catalog

import (
	"reflect"
	"testing"
)

func TestCatalogInterfaceShape(t *testing.T) {
	catalogType := reflect.TypeOf((*Catalog)(nil)).Elem()
	for _, method := range []string{"Metadata", "AccessControl", "Migration", "Close"} {
		if _, ok := catalogType.MethodByName(method); !ok {
			t.Fatalf("Catalog is missing method %s", method)
		}
	}
}

func TestMetadataInterfaceShape(t *testing.T) {
	metadataType := reflect.TypeOf((*MetadataCatalog)(nil)).Elem()
	for _, method := range []string{"Databases", "Collections", "Partitions", "Aliases", "Indexes"} {
		if _, ok := metadataType.MethodByName(method); !ok {
			t.Fatalf("MetadataCatalog is missing method %s", method)
		}
	}
}

func TestDomainGroupShapes(t *testing.T) {
	accessType := reflect.TypeOf((*AccessControlCatalog)(nil)).Elem()
	for _, method := range []string{"Credentials", "Roles", "Grants", "PrivilegeGroups"} {
		if _, ok := accessType.MethodByName(method); !ok {
			t.Fatalf("AccessControlCatalog is missing method %s", method)
		}
	}
}

func TestPublicCatalogDoesNotExposeRawKV(t *testing.T) {
	rawNames := map[string]struct{}{
		"Get": {}, "Put": {}, "Txn": {}, "Watch": {}, "Load": {},
	}
	walk := func(typ reflect.Type) {
		t.Helper()
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Interface {
			return
		}
		for i := 0; i < typ.NumMethod(); i++ {
			m := typ.Method(i)
			if _, bad := rawNames[m.Name]; bad {
				t.Fatalf("%s exposes raw backend method %s", typ.Name(), m.Name)
			}
			for arg := 0; arg < m.Type.NumIn(); arg++ {
				argType := m.Type.In(arg)
				if argType.Name() == "Txn"+"KV" || argType.Name() == "Meta"+"Kv" {
					t.Fatalf("%s.%s exposes backend type %s", typ.Name(), m.Name, argType.Name())
				}
			}
		}
	}
	walk(reflect.TypeOf((*Catalog)(nil)).Elem())
	walk(reflect.TypeOf((*MetadataCatalog)(nil)).Elem())
	walk(reflect.TypeOf((*AccessControlCatalog)(nil)).Elem())
	walk(reflect.TypeOf((*MigrationCatalog)(nil)).Elem())
}
