package extension

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// fakeCoordClient is a no-op CoordClient used only to prove that the client
// handed to an AdmissionChecker method is the same instance the caller passed
// in.
type fakeCoordClient struct{}

func (fakeCoordClient) ListDatabases(context.Context, *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return nil, nil
}

func (fakeCoordClient) ShowCollections(context.Context, *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return nil, nil
}

// fakeAdmissionChecker records the CoordClient it was called with, per
// method, and returns a preconfigured error per method.
type fakeAdmissionChecker struct {
	createCollectionErr  error
	createDatabaseErr    error
	seenCreateCollection CoordClient
	seenCreateDatabase   CoordClient
}

func (f *fakeAdmissionChecker) CheckCreateCollection(ctx context.Context, coord CoordClient) error {
	f.seenCreateCollection = coord
	return f.createCollectionErr
}

func (f *fakeAdmissionChecker) CheckCreateDatabase(ctx context.Context, coord CoordClient) error {
	f.seenCreateDatabase = coord
	return f.createDatabaseErr
}

func TestCapabilitiesReportsAdmissionPresence(t *testing.T) {
	assert.False(t, Capabilities{}.has(CapAdmission),
		"an empty table must not claim to supply the admission capability")
	assert.True(t, Capabilities{Admission: &fakeAdmissionChecker{}}.has(CapAdmission))
}

func TestSetProviderRejectsMissingAdmissionCapability(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapAdmission},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapAdmission))
}

func TestInstalledAdmissionCheckerIsReachableThroughCaps(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	checker := &fakeAdmissionChecker{}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{Admission: checker}}))

	got := Caps().Admission
	assert.NotNil(t, got)

	coord := fakeCoordClient{}

	assert.NoError(t, got.CheckCreateCollection(context.Background(), coord))
	assert.Equal(t, CoordClient(coord), checker.seenCreateCollection,
		"the CoordClient passed to CheckCreateCollection must reach the implementation unchanged")

	assert.NoError(t, got.CheckCreateDatabase(context.Background(), coord))
	assert.Equal(t, CoordClient(coord), checker.seenCreateDatabase,
		"the CoordClient passed to CheckCreateDatabase must reach the implementation unchanged")
}

func TestAdmissionCheckerErrorIsPropagated(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	wantCollErr := errors.New("collection admission rejected")
	wantDBErr := errors.New("database admission rejected")
	checker := &fakeAdmissionChecker{createCollectionErr: wantCollErr, createDatabaseErr: wantDBErr}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{Admission: checker}}))

	coord := fakeCoordClient{}

	collErr := Caps().Admission.CheckCreateCollection(context.Background(), coord)
	assert.ErrorIs(t, collErr, wantCollErr,
		"an error from CheckCreateCollection must survive install, Caps, and the call unwrapped and unreplaced")

	dbErr := Caps().Admission.CheckCreateDatabase(context.Background(), coord)
	assert.ErrorIs(t, dbErr, wantDBErr,
		"an error from CheckCreateDatabase must survive install, Caps, and the call unwrapped and unreplaced")
}
