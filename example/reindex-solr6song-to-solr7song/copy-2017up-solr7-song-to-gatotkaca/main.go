package main

import (
	colorable "github.com/mattn/go-colorable"
	"github.com/raditzlawliet/solr-copy/model"
	"github.com/raditzlawliet/solr-copy/solr"
	log "github.com/sirupsen/logrus"
)

func init() {
	InitLogger()
}

var (
	viewConf model.SolrConfig
	sConf    model.SolrConfig
)

func main() {
	viewConf = model.SolrConfig{
		SourceHost:        "http://192.168.70.220:18983/solr/",
		TargetHost:        "http://192.168.70.220:18983/solr/",
		Source:            "song",
		Target:            "song",
		SourceQuery:       "*:*&sort=id+asc",
		SourceCursorMark:  "*",
		SourceRows:        10000,
		Max:               -1,
		ShowLog:           false, // checking only
		CommitAfterFinish: false, // solr only
		PostingData:       false, // solr only
	}

	sConf = model.SolrConfig{
		SourceHost:        "http://192.168.70.220:18983/solr/",
		TargetHost:        "http://192.168.70.220:18983/solr/",
		Source:            "song",
		Target:            "song_gatotkaca",
		SourceQuery:       "*:*&sort=id+asc&fq=last_release_date:[2017-01-01T00:00:00Z+TO+*]",
		SourceCursorMark:  "*",
		SourceRows:        10000,
		Max:               -1,
		ShowLog:           true,
		CommitAfterFinish: true, // solr only
		PostingData:       true, // solr only
		DataProcessFunc:   DataProcess,
	}

	log.Info("Update SKW Solr-7 Song")
	solr.Copy(sConf)
}

func DataProcess(data map[string]interface{}) (map[string]interface{}, bool, bool) {
	return data, true, true
}

func InitLogger() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true, TimestampFormat: "2006/01/02 15:04:05"})
	log.SetOutput(colorable.NewColorableStdout())
	log.SetLevel(log.InfoLevel)
}
