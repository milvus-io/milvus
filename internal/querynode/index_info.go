package querynode

type indexInfo struct {
	indexName   string
	indexID     UniqueID
	buildID     UniqueID
	indexPaths  []string
	indexParams map[string]string
	readyLoad   bool
}

func newIndexInfo() *indexInfo {
	return &indexInfo{
		indexPaths:  make([]string, 0),
		indexParams: make(map[string]string),
	}
}

func (info *indexInfo) setIndexName(name string) {
	info.indexName = name
}

func (info *indexInfo) setIndexID(id UniqueID) {
	info.indexID = id
}

func (info *indexInfo) setBuildID(id UniqueID) {
	info.buildID = id
}

func (info *indexInfo) setIndexPaths(paths []string) {
	info.indexPaths = paths
}

func (info *indexInfo) setIndexParams(params map[string]string) {
	info.indexParams = params
}

func (info *indexInfo) setReadyLoad(load bool) {
	info.readyLoad = load
}

func (info *indexInfo) getIndexName() string {
	return info.indexName
}

func (info *indexInfo) getIndexID() UniqueID {
	return info.indexID
}

func (info *indexInfo) getBuildID() UniqueID {
	return info.buildID
}

func (info *indexInfo) getIndexPaths() []string {
	return info.indexPaths
}

func (info *indexInfo) getIndexParams() map[string]string {
	return info.indexParams
}

func (info *indexInfo) getReadyLoad() bool {
	return info.readyLoad
}
