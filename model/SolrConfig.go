package model

type SolrConfig struct {
	ID               string
	SourceHost       string
	TargetHost       string // please encode yourself
	Source           string
	Target           string
	SourceQuery      string // please encode yourself
	SourceCursorMark string // please encode yourself
	SourceRows       int
	Max              int // -1
	ShowLog          bool

	CommitAfterFinish bool
	PostingData       bool
	// data, removeVersion, insert
	DataProcessFunc func(map[string]interface{}) (map[string]interface{}, bool, bool)
}
