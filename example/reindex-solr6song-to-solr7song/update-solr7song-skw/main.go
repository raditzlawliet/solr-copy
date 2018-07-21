package main

import (
	gaemonhelper "eaciit/melon/gaemon/helper"
	"fmt"
	"strings"

	helper "github.com/raditzlawliet/solr-copy/example/reindex-solr6song-to-solr7song/helper"

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
		Target:            "song",
		SourceQuery:       "*:*&sort=id+asc",
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
	docType := data["type"]

	// restrict new type
	if docType != "song" && docType != "album" && docType != "artist" && docType != "pl" && docType != "song_vod" {
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

	{
		artistSkw := map[string]string{}
		albumSkw := map[string]string{}
		songSkw := map[string]string{}

		delete(data, "artist_search_keyword")
		delete(data, "album_search_keyword")
		delete(data, "song_search_keyword")

		delete(data, "search_keyword")
		delete(data, "search_keyword_soundex")

		delete(data, "suggest_keyword")

		// update date last release date with reg_date
		lastReleaseDate := gaemonhelper.ParseSolrDateFormat(data["reg_date"].(string))

		if docType == "artist" {
			// take all album
			fieldValue := helper.GetStringFromTkM(data, fieldCheck)
			checkChildConf := viewConf
			checkChildConf.SourceQuery = fmt.Sprintf("*:*&fq=%v:%v&fq=type:%v&sort=id+asc", fieldCheck, fieldValue, "album")
			checkChildConf.ID = checkChildConf.SourceQuery
			checkChildConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
				{
					name := helper.GetStringFromTkM(data, "album_name")
					cleanName := gaemonhelper.FilterSearchKeyword(name, true)
					albumSkw[name] = name
					albumSkw[cleanName] = cleanName
				}
				{
					name := helper.GetStringFromTkM(data, "album_name_origin")
					cleanName := gaemonhelper.FilterSearchKeyword(name, true)
					albumSkw[name] = name
					albumSkw[cleanName] = cleanName

				}
				// update date last release date with reg_date
				_lastReleaseDate := gaemonhelper.ParseSolrDateFormat(_data["reg_date"].(string))
				lastReleaseDate = gaemonhelper.GetCompareSolrDateFormat(lastReleaseDate, _lastReleaseDate, true)
				return nil, false, false
			}
			solr.Copy(checkChildConf)
		}

		if docType == "artist" || docType == "album" {
			// take all song
			fieldValue := helper.GetStringFromTkM(data, fieldCheck)
			checkChildConf := viewConf
			checkChildConf.SourceQuery = fmt.Sprintf("*:*&fq=%v:%v&fq=type:%v&sort=id+asc", fieldCheck, fieldValue, "song")
			checkChildConf.ID = checkChildConf.SourceQuery
			checkChildConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
				{
					name := helper.GetStringFromTkM(data, "song_name")
					cleanName := gaemonhelper.FilterSearchKeyword(name, true)
					songSkw[name] = name
					songSkw[cleanName] = cleanName

				}
				{
					name := helper.GetStringFromTkM(data, "song_name_origin")
					cleanName := gaemonhelper.FilterSearchKeyword(name, true)
					songSkw[name] = name
					songSkw[cleanName] = cleanName

				}
				// update date last release date with reg_date
				_lastReleaseDate := gaemonhelper.ParseSolrDateFormat(_data["reg_date"].(string))
				lastReleaseDate = gaemonhelper.GetCompareSolrDateFormat(lastReleaseDate, _lastReleaseDate, true)
				return nil, false, false
			}
			solr.Copy(checkChildConf)
		}

		if docType == "artist" || docType == "album" || docType == "song" || docType == "song_vod" {
			{
				name := helper.GetStringFromTkM(data, "artist_name")
				cleanName := gaemonhelper.FilterSearchKeyword(name, true)
				artistSkw[name] = name
				artistSkw[cleanName] = cleanName
			}
			{
				name := helper.GetStringFromTkM(data, "artist_name_origin")
				cleanName := gaemonhelper.FilterSearchKeyword(name, true)
				artistSkw[name] = name
				artistSkw[cleanName] = cleanName
			}
			{
				name := helper.GetStringFromTkM(data, "album_name")
				cleanName := gaemonhelper.FilterSearchKeyword(name, true)
				albumSkw[name] = name
				albumSkw[cleanName] = cleanName
			}
			{
				name := helper.GetStringFromTkM(data, "album_name_origin")
				cleanName := gaemonhelper.FilterSearchKeyword(name, true)
				albumSkw[name] = name
				albumSkw[cleanName] = cleanName
			}
			{
				name := helper.GetStringFromTkM(data, "song_name")
				cleanName := gaemonhelper.FilterSearchKeyword(name, true)
				songSkw[name] = name
				songSkw[cleanName] = cleanName
			}
			{
				name := helper.GetStringFromTkM(data, "song_name_origin")
				cleanName := gaemonhelper.FilterSearchKeyword(name, true)
				songSkw[name] = name
				songSkw[cleanName] = cleanName
			}
		}

		if docType == "pl" {
			// take all pl
			plSongs := helper.GetLongsFromTkM(data, "pl_songs")

			type structPLGroup struct {
				plSongs []int64
			}

			plSongsGroups := []structPLGroup{}
			plSongsGroup := structPLGroup{
				plSongs: []int64{},
			}
			// divide song every 20 to 1 group
			for i := 0; i < len(plSongs); i++ {
				plSongsGroup.plSongs = append(plSongsGroup.plSongs, plSongs[i])
				if i != 0 && i%20 == 0 {
					plSongsGroups = append(plSongsGroups, plSongsGroup)
					plSongsGroup = structPLGroup{
						plSongs: []int64{},
					}
				}
			}

			// sisa song
			if len(plSongsGroup.plSongs) > 0 {
				plSongsGroups = append(plSongsGroups, plSongsGroup)
			}

			// repeat after me
			for _, _plSongsGroup := range plSongsGroups {
				// creating query with join OR
				qs := []string{}
				for _, songID := range _plSongsGroup.plSongs {
					qs = append(qs, fmt.Sprintf("id:song-%v", songID))
				}
				q := fmt.Sprintf("(%v)", strings.Join(qs[:], " OR "))

				checkChildConf := viewConf
				checkChildConf.SourceQuery = fmt.Sprintf("%v&fq=type:song&sort=id+asc", q)
				checkChildConf.ID = checkChildConf.SourceQuery
				checkChildConf.DataProcessFunc = func(_data map[string]interface{}) (map[string]interface{}, bool, bool) {
					{
						name := helper.GetStringFromTkM(data, "song_name")
						cleanName := gaemonhelper.FilterSearchKeyword(name, true)
						songSkw[name] = name
						songSkw[cleanName] = cleanName
					}
					{
						name := helper.GetStringFromTkM(data, "song_name_origin")
						cleanName := gaemonhelper.FilterSearchKeyword(name, true)
						songSkw[name] = name
						songSkw[cleanName] = cleanName

					}
					return nil, false, false
				}
				solr.Copy(checkChildConf)
			}
		}

		// update last release date for artist/album/song/song_vod
		data["last_release_date"] = lastReleaseDate
		fmt.Println(data["last_release_date"])

		data["artist_search_keyword"] = gaemonhelper.MapStringStringToSlice(artistSkw)
		data["album_search_keyword"] = gaemonhelper.MapStringStringToSlice(albumSkw)
		data["song_search_keyword"] = gaemonhelper.MapStringStringToSlice(songSkw)

		skwSlice := []string{}
		skwSlice = append(skwSlice, data["artist_search_keyword"].([]string)...)
		skwSlice = append(skwSlice, data["album_search_keyword"].([]string)...)
		skwSlice = append(skwSlice, data["song_search_keyword"].([]string)...)

		data["search_keyword"] = gaemonhelper.GetUniqueStrings(skwSlice)
		data["search_keyword_soundex"] = data["search_keyword"]

		if docType == "song" {
			data["suggest_keyword"] = []string{
				fmt.Sprintf("%v - %v", gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "song_name"), true), gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "artist_name"), true)),
				fmt.Sprintf("%v - %v", gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "artist_name"), true), gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "song_name"), true)),
			}
		} else if docType == "artist" {
			data["suggest_keyword"] = []string{gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "artist_name"), true)}
		} else if docType == "album" {
			data["suggest_keyword"] = []string{gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "album_name"), true)}
		} else if docType == "pl" {
			data["suggest_keyword"] = []string{gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "pl_name"), true)}
		} else if docType == "song_vod" {
			data["suggest_keyword"] = []string{
				gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "vod_title"), true),
				gaemonhelper.FilterSearchKeyword(helper.GetStringFromTkM(data, "song_name"), true),
			}
		}

	}

	removeVersion := true
	insert := true
	return data, removeVersion, insert
}

func InitLogger() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true, TimestampFormat: "2006/01/02 15:04:05"})
	log.SetOutput(colorable.NewColorableStdout())
	log.SetLevel(log.InfoLevel)
}
