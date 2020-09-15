package reader

func startQueryNode(pulsarURL string) {
	qn := NewQueryNode(0, 0)
	qn.InitQueryNodeCollection()
	//go qn.SegmentService()
	qn.StartMessageClient(pulsarURL)

	go qn.RunSearch()
	qn.RunInsertDelete()
}
