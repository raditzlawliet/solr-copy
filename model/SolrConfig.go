package model

type SolrConfig struct {
	SourceHost        string
	TargetHost        string
	Source            string
	Target            string
	SourceQuery       string
	SourceCursorMark  string
	SourceRows        int
	Max               int // -1
	CommitAfterFinish bool
	PostingData       bool
	DataProcessFunc   func(map[string]interface{}) map[string]interface{}
}
