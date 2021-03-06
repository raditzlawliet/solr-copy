package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/araddon/dateparse"
	colorable "github.com/mattn/go-colorable"
	"github.com/raditzlawliet/solr-copy/model"
	"github.com/raditzlawliet/solr-copy/solr"
	log "github.com/sirupsen/logrus"
)

func main() {
	// this library using log logrus
	InitLogger()

	// sample
	// {
	// 	InsertNewData := true
	// 	sConf := model.SolrConfig{
	// 		SourceHost:       "http://192.168.70.230:8983/solr/",
	// 		TargetHost:       "http://192.168.70.230:8983/solr/",
	// 		Source:           "searchLog",
	// 		Target:           "searchLog",
	// 		SourceQuery:      "*:*&sort=date+asc,id+asc&fq=date:[2017-01-01T00:00:00Z%20TO%202017-02-01T00:00:00Z]",
	// 		SourceCursorMark: "*",
	// 		SourceRows:       10000,
	// 		Max:              1,
	// 		ReadOnly:         true,
	// 		DataProcess: func(data map[string]interface{}) map[string]interface{} {
	// 			if datei, ok := data["date"]; ok {
	// 				datestr := datei.(string)
	// 				layout := "2006-01-02T15:04:05Z"
	// 				date, err := time.Parse(layout, datestr)
	// 				if err != nil {
	// 					log.Error(err)
	// 				}
	// 				newDate := date.AddDate(1, 4, 0)
	// 				newDatestr := newDate.Format(layout)
	// 				// log.Debugf("searchLog=%v : query '%v'", newDatestr, data["query"])
	// 				data["date"] = newDatestr
	// 			}
	// 			if InsertNewData {
	// 				delete(data, "id")
	// 			}
	// 			return data
	// 		},
	// 	}
	// 	solr.Copy(sConf)
	// }

	// sample running lib
	{
		// InsertNewData := false
		sConf := model.SolrConfig{
			SourceHost:        "http://192.168.70.220:8983/solr/",
			TargetHost:        "http://192.168.70.220:18983/solr/",
			Source:            "song3",
			Target:            "song_c",
			SourceQuery:       "*:*&sort=id+asc",
			SourceCursorMark:  "*",
			SourceRows:        10000,
			Max:               500000,
			CommitAfterFinish: true, // solr only
			PostingData:       true, // solr only
			DataProcessFunc: func(data map[string]interface{}) (map[string]interface{}, bool, bool) {
				// log.Debug(data)
				docType := data["type"]

				// remove other type
				if docType == "album_properties" || docType == "song_properties" || docType == "artist_properties" {
					return nil, false, false
				}

				artistSkw := map[string]string{}
				albumSkw := map[string]string{}
				songSkw := map[string]string{}
				searchKeyword := map[string]string{}

				delete(data, "artist_search_keyword")
				delete(data, "album_search_keyword")
				delete(data, "song_search_keyword")
				delete(data, "search_keyword")

				if docType == "artist" || docType == "album" || docType == "song" {
					if _, ok := data["artist_name_origin"]; ok {
						name := data["artist_name_origin"].(string)
						name = strings.TrimSpace(name)
						cleanName := CleanString(name)
						artistSkw[name] = name
						artistSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
					if _, ok := data["artist_name"]; ok {
						if name := GetNameFromSlice(data["artist_name"]); name != "" {
							cleanName := CleanString(name)
							artistSkw[name] = name
							artistSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
							data["artist_name"] = name
						}
					}
					// re-index if exist
					// if _, ok := data["artist_search_keyword"]; ok {
					// 	if name := GetNameFromSlice(data["artist_search_keyword"]); name != "" {
					// 		cleanName := CleanString(name)
					// 		artistSkw[name] = name
					// 		artistSkw[cleanName] = cleanName
					// 		searchKeyword[name] = name
					// 		searchKeyword[cleanName] = cleanName
					// 	}
					// }

					_artistSkw := make([]string, 0)
					for _, v := range artistSkw {
						_artistSkw = append(_artistSkw, v)
					}
					data["artist_search_keyword"] = _artistSkw
				}

				if docType == "album" || docType == "song" {
					if _, ok := data["album_name_origin"]; ok {
						if name, ok2 := data["album_name_origin"].(string); ok2 {
							name = strings.TrimSpace(name)
							cleanName := CleanString(name)
							albumSkw[name] = name
							albumSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
						}
					}
					if _, ok := data["album_name"]; ok {
						if name := GetNameFromSlice(data["artist_search_keyword"]); name != "" {
							cleanName := CleanString(name)
							albumSkw[name] = name
							albumSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
							data["album_name"] = name
						}
					}
					// if _, ok := data["album_search_keyword"]; ok {
					// 	if name := GetNameFromSlice(data["album_search_keyword"]); name != "" {
					// 		cleanName := CleanString(name)
					// 		albumSkw[name] = name
					// 		albumSkw[cleanName] = cleanName
					// 		searchKeyword[name] = name
					// 		searchKeyword[cleanName] = cleanName
					// 	}
					// }

					_albumSkw := make([]string, 0)
					for _, v := range albumSkw {
						_albumSkw = append(_albumSkw, v)
					}
					data["album_search_keyword"] = _albumSkw
				}

				if docType == "song" {
					if _, ok := data["song_name_origin"]; ok {
						name := data["song_name_origin"].(string)
						name = strings.TrimSpace(name)
						cleanName := CleanString(name)
						songSkw[name] = name
						songSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
					if _, ok := data["song_name"]; ok {
						if name := GetNameFromSlice(data["song_name"]); name != "" {
							cleanName := CleanString(name)
							songSkw[name] = name
							songSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
							data["song_name"] = name
						}
					}
					// if _, ok := data["song_search_keyword"]; ok {
					// 	if name := GetNameFromSlice(data["song_search_keyword"]); name != "" {
					// 		cleanName := CleanString(name)
					// 		songSkw[name] = name
					// 		songSkw[cleanName] = cleanName
					// 		searchKeyword[name] = name
					// 		searchKeyword[cleanName] = cleanName
					// 	}
					// }

					_songSkw := make([]string, 0)
					for _, v := range songSkw {
						_songSkw = append(_songSkw, v)
					}
					data["song_search_keyword"] = _songSkw
				}

				if docType == "pl" {
					if inames, ok2 := data["search_keyword"].([]interface{}); ok2 {
						for _, iname := range inames {
							name := strings.TrimSpace(iname.(string))
							searchKeyword[name] = name
						}
					} else if inames, ok2 := data["search_keyword"].([]string); ok2 {
						for _, iname := range inames {
							name := strings.TrimSpace(iname)
							searchKeyword[name] = name
						}
					}
					// convert to slice
					_searchKeyword := make([]string, 0)
					for _, v := range searchKeyword {
						_searchKeyword = append(_searchKeyword, v)
					}
					data["search_keyword"] = _searchKeyword
				}

				// convert to slice
				_searchKeyword := make([]string, 0)
				for _, v := range searchKeyword {
					_searchKeyword = append(_searchKeyword, v)
				}
				data["search_keyword"] = _searchKeyword

				// rewrite string to string date format solr
				if _, ok := data["reg_date"]; ok {
					data["reg_date"] = parseSolrDateFormat(data["reg_date"].(string))
				}
				if _, ok := data["upd_date"]; ok {
					data["upd_date"] = parseSolrDateFormat(data["upd_date"].(string))
				}
				if _, ok := data["album_reg_date"]; ok {
					data["album_reg_date"] = parseSolrDateFormat(data["album_reg_date"].(string))
				}

				// if InsertNewData {
				// 	delete(data, "id")
				// }
				// log.Debugf("%v : %v", data["search_keyword"], len(data["search_keyword"].([]string)))
				removeVersion := true
				insert := true
				return data, removeVersion, insert
			},
		}
		solr.Copy(sConf)
	}

}

func parseSolrDateFormat(_date string) string {
	t, err := dateparse.ParseLocal(_date)
	if err != nil {
		fmt.Println(err.Error())
		return _date
	} else {
		format := t.Format("2006-01-02 15:04:05")
		return format
	}
}

func CleanString(input string) string {
	buff := bytes.Buffer{}
	for _, r := range input {
		//fmt.Println(r)
		if r >= 48 && r <= 57 { //check if nnumber
			buff.WriteRune(r)
		} else if (r >= 65 && r <= 90) || (r >= 97 && r <= 122) { //check if letter
			buff.WriteRune(r)
		} else if r == 32 { //space
			buff.WriteRune(r)
		}
	}
	return strings.TrimSpace(buff.String())
}

func GetNameFromSlice(i interface{}) string {
	name := ""
	if iname, ok2 := i.([]interface{}); ok2 {
		if len(iname) > 0 {
			name = iname[0].(string)
			name = strings.TrimSpace(name)
		}
	} else if iname, ok2 := i.([]string); ok2 {
		if len(iname) > 0 {
			name = iname[0]
			name = strings.TrimSpace(name)
		}
	}
	return name
}

func InitLogger() {

	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true, TimestampFormat: "2006/01/02 15:04:05"})
	log.SetOutput(colorable.NewColorableStdout())

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	// log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)

	// logName := "solr-to-mgo"
	// pathFolder := "log"
	// logFileName := logName + ".%Y%m%d-%H%M.log"

	// creating log folder
	// _ = os.MkdirAll(pathFolder, 0766)

	// writer, err := rotatelogs.New(
	// 	filepath.Join(pathFolder, logFileName),
	// 	rotatelogs.WithLinkName(filepath.Join(pathFolder, logFileName)),
	// 	rotatelogs.WithMaxAge(time.Duration(86400)*time.Second),        // default Max 7 days
	// 	rotatelogs.WithRotationTime(time.Duration(604800)*time.Second), // default 24 hour
	// )

	// if err != nil {
	// 	log.Error(err)
	// 	os.Exit(1)
	// }

	// log.AddHook(lfshook.NewHook(
	// 	lfshook.WriterMap{
	// 		log.DebugLevel: writer,
	// 		log.InfoLevel:  writer,
	// 		log.WarnLevel:  writer,
	// 		log.ErrorLevel: writer,
	// 		log.FatalLevel: writer,
	// 		log.PanicLevel: writer,
	// 	},

	// 	&log.JSONFormatter{},
	// 	// &log.TextFormatter{FullTimestamp: true, TimestampFormat: "2006/01/02 15:04:05"},
	// ))
}
