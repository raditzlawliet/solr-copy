package model

type SolrConfig struct {
	ID               string
	SourceHost       string
	TargetHost       string
	Source           string
	Target           string
	SourceQuery      string
	SourceCursorMark string
	SourceRows       int
	Max              int // -1
	ShowLog          bool

	CommitAfterFinish bool
	PostingData       bool
	// data, removeVersion, insert
	DataProcessFunc func(map[string]interface{}) (map[string]interface{}, bool, bool)
}
