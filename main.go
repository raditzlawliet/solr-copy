package main

import (
	"bytes"
	"strings"

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
		InsertNewData := false
		sConf := model.SolrConfig{
			SourceHost:       "http://192.168.70.220:8983/solr/",
			TargetHost:       "http://192.168.70.220:8983/solr/",
			Source:           "song3",
			Target:           "song",
			SourceQuery:      "*:*&sort=id+asc",
			SourceCursorMark: "*",
			SourceRows:       10000,
			Max:              1,
			ReadOnly:         true,
			DataProcess: func(data map[string]interface{}) map[string]interface{} {
				docType := data["type"]
				artistSkw := map[string]string{}
				albumSkw := map[string]string{}
				songSkw := map[string]string{}
				searchKeyword := map[string]string{}

				if docType == "artist" || docType == "album" || docType == "song" {
					if _, ok := data["artist_name_origin"]; ok {
						name := data["artist_name_origin"].(string)
						cleanName := CleanString(name)
						artistSkw[name] = name
						artistSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
					if _, ok := data["artist_name"]; ok {
						name := data["artist_name"].(string)
						cleanName := CleanString(name)
						artistSkw[name] = name
						artistSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}

					_artistSkw := make([]string, 0)
					for _, v := range artistSkw {
						_artistSkw = append(_artistSkw, v)
					}
					data["song_search_keyword"] = _artistSkw
				}

				if docType == "album" || docType == "song" {
					if _, ok := data["album_name_origin"]; ok {
						name := data["album_name_origin"].(string)
						cleanName := CleanString(name)
						albumSkw[name] = name
						albumSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
					if _, ok := data["album_name"]; ok {
						name := data["album_name"].(string)
						cleanName := CleanString(name)
						albumSkw[name] = name
						albumSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}

					_albumSkw := make([]string, 0)
					for _, v := range albumSkw {
						_albumSkw = append(_albumSkw, v)
					}
					data["album_search_keyword"] = _albumSkw
				}

				if docType == "song" {
					if _, ok := data["song_name_origin"]; ok {
						name := data["song_name_origin"].(string)
						cleanName := CleanString(name)
						songSkw[name] = name
						songSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
					if _, ok := data["song_name"]; ok {
						name := data["song_name"].(string)
						cleanName := CleanString(name)
						songSkw[name] = name
						songSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}

					_songSkw := make([]string, 0)
					for _, v := range songSkw {
						_songSkw = append(_songSkw, v)
					}
					data["song_search_keyword"] = _songSkw
				}
				// insert old search_keyword
				_searchKeyword := make([]string, 0)
				for _, v := range searchKeyword {
					_searchKeyword = append(_searchKeyword, v)
				}
				data["search_keyword"] = _searchKeyword

				if InsertNewData {
					delete(data, "id")
				}
				return data
			},
		}
		solr.Copy(sConf)
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
