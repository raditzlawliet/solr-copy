package solr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	. "github.com/raditzlawliet/solr-copy/model"
	log "github.com/sirupsen/logrus"
)

func Copy(conf SolrConfig) {
	CopyID := conf.ID
	SourceHost := conf.SourceHost
	TargetHost := conf.TargetHost
	Source := conf.Source
	Target := conf.Target

	// SourceQueryURL := &url.URL{RawQuery: conf.SourceQuery}
	// u, err := url.Parse(conf.SourceQuery)
	// if err != nil {
	// 	panic(err)
	// }
	// u.RawQuery = u.Query().Encode()
	// SourceQuery := u.RawQuery
	SourceQuery := conf.SourceQuery
	SourceCursorMark := conf.SourceCursorMark
	SourceRows := conf.SourceRows
	Max := conf.Max
	DataProcessFunc := conf.DataProcessFunc

	CommitAfterFinish := conf.CommitAfterFinish
	PostingData := conf.PostingData

	// solr url
	TargetSolrUrlPost := (fmt.Sprintf("%s%s/update/json/docs", TargetHost, Target))
	TargetSolrUrlCommit := (fmt.Sprintf("%s%s/update?commit=true", TargetHost, Target))

	TotalData := 0

	for i := 0; ; i++ {
		end := func() bool {
			SourceCursorMark = strings.Replace(SourceCursorMark, "+", "%2B", -1)
			rowToGet := SourceRows

			// fetch row more than Max
			if rowToGet > Max && Max > 0 {
				rowToGet = Max
			}

			// remaining
			if Max-TotalData < SourceRows && Max > 0 {
				rowToGet = Max - TotalData
			}

			client := http.Client{}
			SourceSolrUrl := (fmt.Sprintf("%s%s/select?q=%s&rows=%v&wt=json&cursorMark=%s", SourceHost, Source, SourceQuery, rowToGet, SourceCursorMark))
			log.Infof("[%v] Getting Data from %v", CopyID, SourceSolrUrl)
			resp, err := client.Get(SourceSolrUrl)
			if err != nil {
				log.Error(err.Error())
				return true
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				log.Errorf("[%v] Error status %v", CopyID, resp.StatusCode)
				res, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Debug(err.Error())
					return true
				}
				log.Debug(string(res))
				return true
			}
			res, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error(err.Error())
				return true
			}

			resMap := map[string]interface{}{}
			err = json.Unmarshal(res, &resMap)
			if err != nil {
				log.Errorf("[%v] error unmarshal %v", CopyID, err.Error())
				log.Debug(string(res))
				return true
			}
			docRes := resMap["response"].(map[string]interface{})
			resNumFound := docRes["numFound"].(float64)
			iDocs := docRes["docs"].([]interface{})

			SourceCursorMark = resMap["nextCursorMark"].(string)

			log.WithFields(log.Fields{
				"length":            len(iDocs),
				"TotalDataFetchs":   TotalData,
				"response.numFound": resNumFound,
				"cursor":            SourceCursorMark,
			}).Infof("[%v] Document Received", CopyID)
			TotalData += len(iDocs)

			// process data
			iNewDocs := []interface{}{}
			for _, doc := range iDocs {
				docMap := doc.(map[string]interface{})
				docMap, removeVersion, insert := DataProcessFunc(docMap)

				if removeVersion {
					delete(docMap, "_version_") // remove version
				}

				if insert {
					iNewDocs = append(iNewDocs, docMap)
				}
			}

			// posting data
			if PostingData {
				docClean := []byte{}
				docClean, _ = json.Marshal(iNewDocs)

				log.WithFields(log.Fields{
					"length": len(iNewDocs),
				}).Debugf("[%v] Posting Data to %v", CopyID, TargetSolrUrlPost)

				resp2, err := client.Post(TargetSolrUrlPost, "application/json", bytes.NewBuffer(docClean))
				if err != nil {
					log.Fatalf("[%v] fail post", CopyID, err.Error())
					os.Exit(1)
				}

				defer resp2.Body.Close()

				if resp2.StatusCode != 200 {
					log.Debug(resp.StatusCode)
					oo, _ := ioutil.ReadAll(resp2.Body)
					log.Fatal(string(oo))
					os.Exit(1)
				}
				// end posting data
			}

			// log.WithFields(log.Fields{
			// 	"cursor":          SourceCursorMark,
			// 	"TotalDataFetchs": TotalData,
			// }).Infof("[%v] Cursor Mark", CopyID)

			// sometime data not return as expected rows, but return cursor
			if len(iDocs) <= 0 && SourceCursorMark != "" {
				return true
			}

			if TotalData >= Max && Max >= 0 {
				return true
			}

			return false
		}()

		if end {
			break
		}
	}
	// commit
	if CommitAfterFinish {
		log.Infof("[%v] Commit Data: %v", CopyID, TargetSolrUrlCommit)
		client := http.Client{}
		resp, err := client.Get(TargetSolrUrlCommit)
		log.Debug(TargetSolrUrlCommit)
		if err != nil {
			log.Error(err.Error())
			return
		}
		if resp.StatusCode != 200 {
			log.Errorf("[%v] Error status %v", CopyID, resp.StatusCode)
			res, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Debug(err.Error())
				return
			}
			resp.Body.Close()
			log.Debug(string(res))
			return
		}
		res, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Error(err.Error())
			return
		}
		resp.Body.Close()

		resMap := map[string]interface{}{}
		err = json.Unmarshal(res, &resMap)

		log.Debug(resMap)
		log.Infof("[%v] Commit Target Solr OK", CopyID)
	}

	log.Infof("[%v] === Done === ", CopyID)

}
