package main

import (
	gaemonhelper "eaciit/melon/gaemon/helper"
	gaemonmodel "eaciit/melon/gaemon/model"
	"fmt"

	helper "github.com/raditzlawliet/solr-copy/example/reindex-solr6song-to-solr7song/helper"

	"github.com/eaciit/toolkit"
	colorable "github.com/mattn/go-colorable"
	"github.com/raditzlawliet/solr-copy/model"
	"github.com/raditzlawliet/solr-copy/solr"
	log "github.com/sirupsen/logrus"
)

func init() {
	InitLogger()
}

func main() {
	sConf := model.SolrConfig{
		SourceHost:        "http://192.168.70.220:8983/solr/",
		TargetHost:        "http://192.168.70.220:18983/solr/",
		Source:            "songs3_201807",
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
	log.Info("Begin Reindex Solr-6 song to Solr-7 song new schema")
	solr.Copy(sConf)
}

func DataProcess(data map[string]interface{}) (map[string]interface{}, bool, bool) {
	docType := data["type"]

	// restrict new type
	if docType != "song" && docType != "album" && docType != "artist" && docType != "pl" && docType != "song_vod" {
		return nil, false, false
	}

	song := gaemonmodel.SongDocumentSolr{}
	{
		CopySong6TkMToSong7(&song, data)

		// moved to update-solr7songs-kw
		// search_keyword
		song.SONG_SEARCH_KEYWORD = []string{song.SONG_NAME, gaemonhelper.FilterSearchKeyword(song.SONG_NAME, true)}
		song.ALBUM_SEARCH_KEYWORD = []string{song.ALBUM_NAME, gaemonhelper.FilterSearchKeyword(song.ALBUM_NAME, true)}
		song.ARTIST_SEARCH_KEYWORD = []string{song.ARTIST_NAME, gaemonhelper.FilterSearchKeyword(song.ARTIST_NAME, true)}

		// // append search_keyword from solr (COMMENT if dont want)
		// if true {
		// 	song.SONG_SEARCH_KEYWORD = append(song.SONG_SEARCH_KEYWORD, strings.Split(songOracle.SEARCH_KEYWORD.String, ",")...)
		// 	song.ALBUM_SEARCH_KEYWORD = append(song.ALBUM_SEARCH_KEYWORD, strings.Split(songOracle.ALBUM_SEARCH_KEYWORD.String, ",")...)
		// 	song.ARTIST_SEARCH_KEYWORD = append(song.ARTIST_SEARCH_KEYWORD, strings.Split(songOracle.ARTIST_SEARCH_KEYWORD.String, ",")...)
		// }

		// cleaning duplicates
		song.SONG_SEARCH_KEYWORD = gaemonhelper.GetUniqueStrings(song.SONG_SEARCH_KEYWORD)
		song.ALBUM_SEARCH_KEYWORD = gaemonhelper.GetUniqueStrings(song.ALBUM_SEARCH_KEYWORD)
		song.ARTIST_SEARCH_KEYWORD = gaemonhelper.GetUniqueStrings(song.ARTIST_SEARCH_KEYWORD)

		song.SEARCH_KEYWORD = []string{}
		song.SEARCH_KEYWORD = append(song.SEARCH_KEYWORD, song.SONG_SEARCH_KEYWORD...)
		song.SEARCH_KEYWORD = append(song.SEARCH_KEYWORD, song.ALBUM_SEARCH_KEYWORD...)
		song.SEARCH_KEYWORD = append(song.SEARCH_KEYWORD, song.ARTIST_SEARCH_KEYWORD...)

		// soundex copyfield manual
		song.SEARCH_KEYWORD_SOUNDEX = song.SEARCH_KEYWORD

		// suggest format SONG - ARTIST | ARTIST - SONG
		if docType == "song" {
			song.SUGGEST_KEYWORD = []string{
				fmt.Sprintf("%v - %v", gaemonhelper.FilterSearchKeyword(song.SONG_NAME, true), gaemonhelper.FilterSearchKeyword(song.ARTIST_NAME, true)),
				fmt.Sprintf("%v - %v", gaemonhelper.FilterSearchKeyword(song.ARTIST_NAME, true), gaemonhelper.FilterSearchKeyword(song.SONG_NAME, true)),
			}
		} else if docType == "artist" {
			song.SUGGEST_KEYWORD = []string{song.ARTIST_NAME}
		} else if docType == "album" {
			song.SUGGEST_KEYWORD = []string{song.ALBUM_NAME}
		} else if docType == "pl" {
			song.SUGGEST_KEYWORD = []string{song.PL_NAME}
		} else if docType == "song_vod" {
			song.SUGGEST_KEYWORD = []string{song.VOD_TITLE}
		}

	}

	songTkM := song.ToM()

	log.Debug(songTkM)

	removeVersion := true
	insert := true
	return songTkM, removeVersion, insert
}

// song solr 6 to song solr 7 struct
func CopySong6TkMToSong7(song *gaemonmodel.SongDocumentSolr, data toolkit.M) {
	// basic all
	if _, ok := data["song_id"]; ok {
		song.SONG_ID = gaemonhelper.GetFirstStringFromSlice(data["song_id"])
	}
	if _, ok := data["song_name"]; ok {
		song.SONG_NAME = gaemonhelper.GetFirstStringFromSlice(data["song_name"])
	}
	if _, ok := data["song_name_origin"]; ok {
		song.SONG_NAME_ORIGIN = gaemonhelper.GetFirstStringFromSlice(data["song_name_origin"])
	}
	// for song/album
	if _, ok := data["album_id"]; ok {
		song.ALBUM_ID = gaemonhelper.GetFirstStringFromSlice(data["album_id"])
	}
	if _, ok := data["main_album_id"]; ok { // ALTERNATIVE
		song.ALBUM_ID = gaemonhelper.GetFirstStringFromSlice(data["main_album_id"])
	}
	if _, ok := data["album_name"]; ok {
		song.ALBUM_NAME = gaemonhelper.GetFirstStringFromSlice(data["album_name"])
	}
	if _, ok := data["album_name_origin"]; ok {
		song.ALBUM_NAME_ORIGIN = gaemonhelper.GetFirstStringFromSlice(data["album_name_origin"])
	}
	if _, ok := data["artist_id"]; ok {
		song.ARTIST_ID = gaemonhelper.GetFirstStringFromSlice(data["artist_id"])
	}
	if _, ok := data["main_artist_id"]; ok { // ALTERNATIVE
		song.ARTIST_ID = gaemonhelper.GetFirstStringFromSlice(data["main_artist_id"])
	}
	if _, ok := data["artist_name"]; ok {
		song.ARTIST_NAME = gaemonhelper.GetFirstStringFromSlice(data["artist_name"])
	}
	if _, ok := data["artist_name_origin"]; ok {
		song.ARTIST_NAME_ORIGIN = gaemonhelper.GetFirstStringFromSlice(data["artist_name_origin"])
	}

	// song
	if _, ok := data["album_status"]; ok {
		song.ALBUM_STATUS = gaemonhelper.GetFirstStringFromSlice(data["album_status"])
	}
	if _, ok := data["artist_status"]; ok {
		song.ARTIST_STATUS = gaemonhelper.GetFirstStringFromSlice(data["artist_status"])
	}
	if _, ok := data["lc_status_cd"]; ok {
		song.LC_STATUS_CD = gaemonhelper.GetFirstStringFromSlice(data["lc_status_cd"])
	}
	if _, ok := data["genid_cd"]; ok {
		song.GENID_CD = gaemonhelper.GetFirstStringFromSlice(data["genid_cd"])
	}
	song.TEXT_LYRIC_YN = helper.GetStringFromTkM(data, "text_lyric_yn")

	// album/artist
	if _, ok := data["status"]; ok {
		song.STATUS = gaemonhelper.GetFirstStringFromSlice(data["status"])
	}
	if _, ok := data["album_type_cd"]; ok {
		song.ALBUM_TYPE_CD = gaemonhelper.GetFirstStringFromSlice(data["album_type_cd"])
	}
	if _, ok := data["domestic_yn"]; ok {
		song.DOMESTIC_YN = gaemonhelper.GetFirstStringFromSlice(data["domestic_yn"])
	}
	if _, ok := data["gender"]; ok {
		song.GENDER = gaemonhelper.GetFirstStringFromSlice(data["gender"])
	}
	if _, ok := data["group_yn"]; ok {
		song.GROUP_YN = gaemonhelper.GetFirstStringFromSlice(data["group_yn"])
	}

	// pl
	song.PL_NAME = helper.GetStringFromTkM(data, "pl_name")
	if _, ok := data["pl_id"]; ok {
		song.PL_ID = gaemonhelper.GetFirstStringFromSlice(data["pl_id"])
	}
	if _, ok := data["creater_id"]; ok {
		song.CREATER_ID = gaemonhelper.GetFirstStringFromSlice(data["creater_id"])
	}
	if _, ok := data["mbp_id"]; ok {
		song.MBP_ID = gaemonhelper.GetFirstStringFromSlice(data["mbp_id"])
	}
	if _, ok := data["user_id"]; ok {
		song.USER_ID = gaemonhelper.GetFirstStringFromSlice(data["user_id"])
	}
	song.PL_ALBUM = helper.GetLongsFromTkM(data, "pl_album")
	song.PL_ARTIST = helper.GetLongsFromTkM(data, "pl_artist")
	song.PL_SONG = helper.GetLongsFromTkM(data, "pl_song")
	song.SHARED_PL_YN = helper.GetStringFromTkM(data, "shared_pl_yn")

	// song_vod
	if _, ok := data["song_vod_id"]; ok {
		song.SONG_VOD_ID = gaemonhelper.GetFirstStringFromSlice(data["song_vod_id"])
	}
	if _, ok := data["label_company"]; ok {
		song.LABEL_COMPANY = gaemonhelper.GetFirstStringFromSlice(data["label_company"])
	}
	song.VOD_TITLE = helper.GetStringFromTkM(data, "vod_title")

	// etc
	if _, ok := data["mod_yn"]; ok {
		song.MOD_YN = gaemonhelper.GetFirstStringFromSlice(data["mod_yn"])
	}
	if _, ok := data["vod_yn"]; ok {
		song.VOD_YN = gaemonhelper.GetFirstStringFromSlice(data["vod_yn"])
	}
	if _, ok := data["genre_id"]; ok {
		song.GENRE_ID = gaemonhelper.GetFirstStringFromSlice(data["genre_id"])
	}
	if _, ok := data["label_cd"]; ok {
		song.LABEL_CD = gaemonhelper.GetFirstStringFromSlice(data["label_cd"])
	}
	if _, ok := data["label_name"]; ok {
		song.LABEL_NAME = gaemonhelper.GetFirstStringFromSlice(data["label_name"])
	}
	song.GENRE_KEYWORD = helper.GetStringsFromTkM(data, "genre_keyword")

	song.BLACKLIST = helper.GetStringsFromTkM(data, "blacklist")
	song.POPULARITY = helper.GetLongFromTkM(data, "popularity")
	// no need, we will process again
	// if _, ok := data["search_keyword"]; ok {
	// 	song.SEARCH_KEYWORD = data["search_keyword"].([]string)
	// }

	// general date
	if _, ok := data["issue_date"]; ok {
		song.ISSUE_DATE = gaemonhelper.ParseSolrDateFormat(data["issue_date"].(string))
	}
	if _, ok := data["reg_date"]; ok {
		song.REG_DATE = gaemonhelper.ParseSolrDateFormat(data["reg_date"].(string))
	}
	if _, ok := data["upd_date"]; ok {
		song.UPD_DATE = gaemonhelper.ParseSolrDateFormat(data["upd_date"].(string))
	}

	song.TYPE = gaemonhelper.GetFirstStringFromSlice(data["type"].(string))
	if song.TYPE == "song" {
		song.ID = fmt.Sprintf("%s-%s", song.TYPE, song.SONG_ID)
	} else if song.TYPE == "album" {
		song.ID = fmt.Sprintf("%s-%s", song.TYPE, song.ALBUM_ID)
	} else if song.TYPE == "artist" {
		song.ID = fmt.Sprintf("%s-%s", song.TYPE, song.ARTIST_ID)
	} else if song.TYPE == "pl" {
		song.ID = fmt.Sprintf("%s-%s-%s", song.TYPE, song.MBP_ID, song.PL_ID)
	} else if song.TYPE == "song_vod" {
		song.ID = fmt.Sprintf("%s-%s", song.TYPE, song.SONG_VOD_ID)
	} else {
		song.ID = gaemonhelper.GetFirstStringFromSlice(data["id"].(string))
	}
}

func InitLogger() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true, TimestampFormat: "2006/01/02 15:04:05"})
	log.SetOutput(colorable.NewColorableStdout())
	log.SetLevel(log.DebugLevel)
}
