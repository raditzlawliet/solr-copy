# Solr Copy
Golang library for copy collection from Solr to Solr/MongoDB with Solr API. This project already using dep as package manager.

## Available Source 
- Solr 6.6

## Available Target 
- Solr 6.6
- MongoDB 3.6

## Sample
Example copy old data into new data with modified date into new date
```go
{
	// we want to insert new data (with old data) instead re-indexing
	InsertNewData := true
	sConf := model.SolrConfig{
		SourceHost:       "http://192.168.70.230:8983/solr/",
		TargetHost:       "http://192.168.70.230:8983/solr/",
		Source:           "searchLog",
		Target:           "searchLog",
		SourceQuery:      "*:*&sort=date+asc,id+asc&fq=date:[2017-01-01T00:00:00Z%20TO%202017-02-01T00:00:00Z]",
		SourceCursorMark: "*", 
		SourceRows:       10000,
		Max:              1, // -1 / 0 to fetch all data
		ReadOnly:         true, // false to commit after process
		DataProcess: func(data map[string]interface{}) map[string]interface{} {
			if datei, ok := data["date"]; ok {
				datestr := datei.(string)
				layout := "2006-01-02T15:04:05Z"
				date, err := time.Parse(layout, datestr)
				if err != nil {
					log.Error(err)
				}
				newDate := date.AddDate(1, 4, 0)
				newDatestr := newDate.Format(layout)
				data["date"] = newDatestr
			}
			if InsertNewData {
				delete(data, "id") 
			}
			return data
		},
	}
	solr.Copy(sConf)
}
```
