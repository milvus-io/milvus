package reader

import "C"

type planCache struct {
	planBuffer map[DSL]plan
}

type plan struct {
	//cPlan C.CPlan
}

func (ss *searchService) Plan(queryBlob string) *plan {
	/*
		@return pointer of plan
		void* CreatePlan(const char* dsl)
	*/

	/*
		CPlaceholderGroup* ParserPlaceholderGroup(const char* placeholders_blob)
	*/


	/*
		long int GetNumOfQuery(CPlaceholderGroup* placeholder_group)

	    long int GetTopK(CPlan* plan)
	 */

	return nil
}
