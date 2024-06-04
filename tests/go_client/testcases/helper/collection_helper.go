package helper

type CreateCollectionParams struct {
	CollectionFieldsType CollectionFieldsType // collection fields type
}

func NewCreateCollectionParams(collectionFieldsType CollectionFieldsType) *CreateCollectionParams {
	return &CreateCollectionParams{
		CollectionFieldsType: collectionFieldsType,
	}
}
