package reader

func startQueryNode() {
	qn := NewQueryNode(0, 0)
	qn.InitQueryNodeCollection()
	go qn.SegmentService()
	qn.StartMessageClient()

	go qn.RunInsertDelete()
	go qn.RunSearch()
}
