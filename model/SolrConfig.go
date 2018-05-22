package model

type SolrConfig struct {
	SourceHost       string
	TargetHost       string
	Source           string
	Target           string
	SourceQuery      string
	SourceCursorMark string
	SourceRows       int
	Max              int // -1
	ReadOnly         bool
	DataProcess      func(map[string]interface{}) map[string]interface{}
}
