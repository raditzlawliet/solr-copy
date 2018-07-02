package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime/trace"
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

	IdDupe := map[string]string{}
	IdToRemove := []string{}

	// sample running lib
	{
		basicConf := model.SolrConfig{
			SourceHost:        "http://192.168.70.220:8983/solr/",
			TargetHost:        "http://192.168.70.220:18983/solr/",
			Source:            "song",
			Target:            "song_full",
			SourceQuery:       "*:*&sort=id+asc",
			SourceCursorMark:  "*",
			SourceRows:        10000,
			Max:               -1,
			ShowLog:           true,
			CommitAfterFinish: true, // solr only
			PostingData:       true, // solr only
		}

		checkConf := basicConf
		checkConf.CommitAfterFinish = false
		checkConf.PostingData = false
		checkConf.ShowLog = false

		sConf := basicConf
		sConf.ID = "Main"
		sConf.DataProcessFunc = func(data map[string]interface{}) (map[string]interface{}, bool, bool) {
			docId := data["id"].(string)
			docType := data["type"].(string)

			// log.Debugf("%v:%v %v", docId, docType, docType != "artist" && docType != "album" && docType != "song" && docType != "pl")

			// remove type yg tidak diperlukan
			if docType != "artist" && docType != "album" && docType != "song" && docType != "pl" {
				return nil, false, false
			}

			// check dupe
			if _, ok := IdDupe[docId]; ok {
				IdToRemove = append(IdToRemove, docId)
				return nil, false, false
			}

			fieldCheck := ""
			if docType == "artist" {
				fieldCheck = "artist_id"
			} else if docType == "album" {
				fieldCheck = "album_id"
			} else if docType == "song" {
				fieldCheck = "song_id"
			} else if docType == "pl" {
				fieldCheck = "pl_id"
			}

			// check dupe
			if fieldCheck != "" {
				if _, ok := data[fieldCheck]; ok {
					fieldValue := data[fieldCheck].(string)

					checkDupeConf := checkConf
					checkDupeConf.ID = fmt.Sprintf("Dupe-%v:%v&type:%v", fieldCheck, fieldValue, docType)
					checkDupeConf.SourceQuery = fmt.Sprintf("%v:%v&type:%v&sort=id+asc", fieldCheck, fieldValue, docType)
					checkDupeConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
						_docId := _data["id"].(string)
						if _docId != docId {
							IdDupe[_docId] = docId
							IdToRemove = append(IdToRemove, docId)
						}
						return nil, false, false
					}
					solr.Copy(checkDupeConf)
				} else {
					// no song/artist/album/pl_id to check within type ? remove it doc
					IdToRemove = append(IdToRemove, docId)
					return nil, false, false
				}
			}

			artistSkw := map[string]string{}
			albumSkw := map[string]string{}
			songSkw := map[string]string{}
			searchKeyword := map[string]string{}

			delete(data, "artist_search_keyword")
			delete(data, "album_search_keyword")
			delete(data, "song_search_keyword")

			delete(data, "search_keyword")
			delete(data, "search_keyword_soundex")

			if docType == "artist" || docType == "album" || docType == "song" {
				if _, ok := data["artist_name_origin"]; ok {
					if name := GetNameFromSlice(data["artist_name_origin"]); name != "" {
						cleanName := CleanString(name)
						artistSkw[name] = name
						artistSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
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
			}

			if docType == "album" || docType == "song" {
				if _, ok := data["album_name_origin"]; ok {
					if name := GetNameFromSlice(data["album_name_origin"]); name != "" {
						cleanName := CleanString(name)
						albumSkw[name] = name
						albumSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
				}
				if _, ok := data["album_name"]; ok {
					if name := GetNameFromSlice(data["album_name"]); name != "" {
						cleanName := CleanString(name)
						albumSkw[name] = name
						albumSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
						data["album_name"] = name
					}
				}
			} else if docType == "artist" {
				// insert album into artist skw
				fieldValue := data[fieldCheck].(string)
				checkChildConf := checkConf
				checkChildConf.ID = fmt.Sprintf("Child-%v:%v&type:%v", fieldCheck, fieldValue, "album")
				checkChildConf.SourceQuery = fmt.Sprintf("%v:%v&type:%v&sort=id+asc", fieldCheck, fieldValue, "album")
				checkChildConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
					if _, ok := _data["album_name_origin"]; ok {
						if name := GetNameFromSlice(_data["album_name_origin"]); name != "" {
							cleanName := CleanString(name)
							albumSkw[name] = name
							albumSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
						}
					}
					if _, ok := _data["album_name"]; ok {
						if name := GetNameFromSlice(_data["album_name"]); name != "" {
							cleanName := CleanString(name)
							albumSkw[name] = name
							albumSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
							data["album_name"] = name
						}
					}
					return nil, false, false
				}
				solr.Copy(checkChildConf)
			}

			if docType == "song" {
				if _, ok := data["song_name_origin"]; ok {
					if name := GetNameFromSlice(data["song_name_origin"]); name != "" {
						cleanName := CleanString(name)
						songSkw[name] = name
						songSkw[cleanName] = cleanName
						searchKeyword[name] = name
						searchKeyword[cleanName] = cleanName
					}
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
			} else if docType == "artist" || docType == "album" {
				// insert song into artist/album type skw
				fieldValue := data[fieldCheck].(string)
				checkChildConf := checkConf
				checkChildConf.ID = fmt.Sprintf("Child-%v:%v&type:%v", fieldCheck, fieldValue, "song")
				checkChildConf.SourceQuery = fmt.Sprintf("%v:%v&type:%v&sort=id+asc", fieldCheck, fieldValue, "song")
				checkChildConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
					if _, ok := _data["song_name_origin"]; ok {
						if name := GetNameFromSlice(data["song_name_origin"]); name != "" {
							cleanName := CleanString(name)
							songSkw[name] = name
							songSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
						}
					}
					if _, ok := _data["song_name"]; ok {
						if name := GetNameFromSlice(_data["song_name"]); name != "" {
							cleanName := CleanString(name)
							songSkw[name] = name
							songSkw[cleanName] = cleanName
							searchKeyword[name] = name
							searchKeyword[cleanName] = cleanName
						}
					}
					return nil, false, false
				}
				solr.Copy(checkChildConf)
			}

			if docType == "pl" {
				plSongs := map[int64]int64{}
				if __d, ok := data["pl_songs"]; ok {
					plSongs[__d.(int64)] = __d.(int64)
				}
				for _, plSongId := range plSongs {
					checkChildConf := checkConf
					checkChildConf.ID = fmt.Sprintf("Child-%v:%v&type:%v", "song_id", plSongId, "song")
					checkChildConf.SourceQuery = fmt.Sprintf("%v:%v&type:%v&sort=id+asc", "song_id", plSongId, "song")
					checkChildConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
						if _, ok := _data["artist_name_origin"]; ok {
							if name := GetNameFromSlice(data["artist_name_origin"]); name != "" {
								cleanName := CleanString(name)
								artistSkw[name] = name
								artistSkw[cleanName] = cleanName
								searchKeyword[name] = name
								searchKeyword[cleanName] = cleanName
							}
						}
						if _, ok := _data["artist_name"]; ok {
							if name := GetNameFromSlice(_data["artist_name"]); name != "" {
								cleanName := CleanString(name)
								artistSkw[name] = name
								artistSkw[cleanName] = cleanName
								searchKeyword[name] = name
								searchKeyword[cleanName] = cleanName
							}
						}
						if _, ok := _data["album_name_origin"]; ok {
							if name := GetNameFromSlice(data["album_name_origin"]); name != "" {
								cleanName := CleanString(name)
								albumSkw[name] = name
								albumSkw[cleanName] = cleanName
								searchKeyword[name] = name
								searchKeyword[cleanName] = cleanName
							}
						}
						if _, ok := _data["album_name"]; ok {
							if name := GetNameFromSlice(_data["album_name"]); name != "" {
								cleanName := CleanString(name)
								albumSkw[name] = name
								albumSkw[cleanName] = cleanName
								searchKeyword[name] = name
								searchKeyword[cleanName] = cleanName
							}
						}
						if _, ok := _data["song_name_origin"]; ok {
							if name := GetNameFromSlice(data["song_name_origin"]); name != "" {
								cleanName := CleanString(name)
								songSkw[name] = name
								songSkw[cleanName] = cleanName
								searchKeyword[name] = name
								searchKeyword[cleanName] = cleanName
							}
						}
						if _, ok := _data["song_name"]; ok {
							if name := GetNameFromSlice(_data["song_name"]); name != "" {
								cleanName := CleanString(name)
								songSkw[name] = name
								songSkw[cleanName] = cleanName
								searchKeyword[name] = name
								searchKeyword[cleanName] = cleanName
							}
						}
						return nil, false, false
					}
					solr.Copy(checkChildConf)
				}
			}

			_artistSkw := make([]string, 0)
			for _, v := range artistSkw {
				_artistSkw = append(_artistSkw, v)
			}
			data["artist_search_keyword"] = _artistSkw

			_songSkw := make([]string, 0)
			for _, v := range songSkw {
				_songSkw = append(_songSkw, v)
			}
			data["song_search_keyword"] = _songSkw

			_albumSkw := make([]string, 0)
			for _, v := range albumSkw {
				_albumSkw = append(_albumSkw, v)
			}
			data["album_search_keyword"] = _albumSkw

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

			removeVersion := true
			insert := true
			return data, removeVersion, insert
		}
		solr.Copy(sConf)
	}

	// tracing solr id duplicate/remove only
	f, err := os.Create("solr_id_info.out")
	if err != nil {
		panic(err)
	}
	trace.Start(f)

	fmt.Println("Solr id duplicated")
	for id, originalId := range IdDupe {
		fmt.Println(fmt.Sprintf("%s ==> %s", originalId, id))
	}

	fmt.Println("solr id not included / deleted")
	for id := range IdToRemove {
		fmt.Println(id)
	}

	defer trace.Stop()

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
	} else if iname, ok2 := i.(string); ok2 {
		name = iname
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
	log.SetLevel(log.InfoLevel)
}
