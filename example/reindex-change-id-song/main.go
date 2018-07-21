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

	// sample running lib
	{
		// InsertNewData := false
		sConf := model.SolrConfig{
			SourceHost:        "http://192.168.70.220:18983/solr/",
			TargetHost:        "http://192.168.70.220:18983/solr/",
			Source:            "song_full",
			Target:            "song_full_newid",
			SourceQuery:       "*:*&sort=id+asc",
			SourceCursorMark:  "*",
			SourceRows:        10000,
			Max:               -1,
			ShowLog:           true,
			CommitAfterFinish: true, // solr only
			PostingData:       true, // solr only
			DataProcessFunc: func(data map[string]interface{}) (map[string]interface{}, bool, bool) {
				// log.Debug(data)
				docType := data["type"]

				// remove other type
				if docType == "album_properties" || docType == "song_properties" || docType == "artist_properties" {
					return nil, false, false
				}

				if docType == "artist" {
					if _, ok := data["artist_id"]; ok {
						data["id"] = fmt.Sprintf("%v-%v", docType, data["artist_id"])
						return data, true, true
					}
					return data, false, false
				}
				if docType == "album" {
					if _, ok := data["album_id"]; ok {
						data["id"] = fmt.Sprintf("%v-%v", docType, data["album_id"])
						return data, true, true
					}
					return data, false, false
				}
				if docType == "song" {
					if _, ok := data["song_id"]; ok {
						data["id"] = fmt.Sprintf("%v-%v", docType, data["song_id"])
						return data, true, true
					}
					return data, false, false
				}
				if docType == "pl" {
					if _, ok := data["pl_id"]; ok {
						if _, ok2 := data["mbp_id"]; ok2 {
							data["id"] = fmt.Sprintf("%v-%v-%v", docType, data["mbp_id"], data["pl_id"])
							return data, true, true
						}
					}
				}
				return data, false, false
			},
		}
		log.Info("Start")
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
