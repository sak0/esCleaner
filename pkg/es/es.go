package es

import (
	"github.com/elastic/go-elasticsearch"
	"github.com/golang/glog"
	"bytes"
	"encoding/json"
	"context"
	"fmt"
	"time"
	"github.com/elastic/go-elasticsearch/esapi"
	"strings"
	"strconv"
)

const (
	defaultPageSize 	= 1000
)

type InnerDelete struct {
	Index string `json:"_index"`
	Type  string `json:"_type"`
	ID    string `json:"_id"`
}

type DeleteBody struct {
	Delete InnerDelete `json:"delete"`
}

type GenericResponse struct {
	ScrollID 	string `json:"_scroll_id,omitempty"`
	Hits		GenericHits	`json:"hits"`
}

type GenericHits struct {
	Total    int     		`json:"total"`
	MaxScore float64 		`json:"max_score"`
	Hits     []GenericHit	`json:"hits"`
}

type GenericHit struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	ID     string  `json:"_id"`
	Score  float64 `json:"_score"`
}

type Esssss struct {
	indexName 	string
	timeField 	string
	dateStart 	int
	dateEnd 	int
	pageSize 	int
	c 			*elasticsearch.Client
}

func New(addrs []string, indexName, timeField string, dateStart, dateEnd int) (*Esssss, error) {
	esConfig := elasticsearch.Config{
		Addresses: addrs,
	}
	esClient, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		glog.V(2).Infof("Error creating the client: %s", err)
		return nil, err
	}

	return &Esssss {
		indexName:indexName,
		timeField:timeField,
		dateEnd:dateEnd,
		dateStart:dateStart,
		pageSize:defaultPageSize,
		c:esClient,
	}, nil
}

func (e *Esssss) getDeleteIds(hits GenericHits) []string {
	ids := []string{}
	for _, hit := range hits.Hits {
		ids = append(ids, hit.ID)
	}

	return ids
}

func (e *Esssss) streamDeleteIds(stop, input chan interface{}) chan interface{} {
	outStream := make(chan interface{})

	go func() {
		defer close(outStream)

		for {
			select {
			case <-stop:
				return
			case obj, ok := <-input:
				if !ok {
					return
				}
				ids, ok := obj.([]string)
				if !ok {
					glog.V(2).Infof("invalid input: %v", obj)
					continue
				}
				bodyString := ""

				for _, id := range ids {
					if id == "" {
						continue
					}

					deleteReq := DeleteBody{
						Delete: InnerDelete{
							Index: 	e.indexName,
							Type: "type1",
							ID:id,
						},
					}
					var buffer bytes.Buffer
					if err := json.NewEncoder(&buffer).Encode(deleteReq); err != nil {
						glog.V(2).Infof("encode failed: %v", err)
						continue
					}
					bodyString += buffer.String()
				}
				req := esapi.BulkRequest{
					Refresh:    "false",
					Body: strings.NewReader(bodyString),
				}

				res, err := req.Do(context.Background(), e.c)
				if err != nil {
					glog.V(2).Infof("INSERT data failed: %v", err)
					continue
				}
				var data interface{}
				if json.NewDecoder(res.Body).Decode(&data); err != nil {
					glog.V(2).Infof("decode failed: %v", err)
					continue
				}
				glog.V(5).Infof("%v %v", res.StatusCode, data)
				res.Body.Close()
				outStream<- ids
			}
		}
	}()

	return outStream
}

func (e *Esssss) streamGetIdsToDeleted(stop chan interface{}, dateToDelete string) chan interface{} {
	outStream := make(chan interface{})

	go func() {
		defer close(outStream)

		var buffer bytes.Buffer
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []map[string]interface{}{
						{
							"term": map[string]interface{}{
								fmt.Sprintf("%s", e.timeField): fmt.Sprintf("%s", dateToDelete),
							},
						},
					},
				},
			},
			"from": 0,
			"size": e.pageSize,
		}
		if err := json.NewEncoder(&buffer).Encode(query); err != nil {
			glog.V(2).Infof("error encoding query: %s", err)
			panic(err)
		}

		res, err := e.c.Search(
			e.c.Search.WithContext(context.Background()),
			e.c.Search.WithIndex(e.indexName),
			e.c.Search.WithBody(&buffer),
			e.c.Search.WithTrackTotalHits(true),
			e.c.Search.WithPretty(),
			e.c.Search.WithScroll(10 * time.Minute),
		)
		if err != nil {
			glog.V(2).Infof("search error : %v", err)
			panic(err)
		}
		defer res.Body.Close()
		glog.V(5).Infof("res %v", res)

		var scrollResp GenericResponse
		if err := json.NewDecoder(res.Body).Decode(&scrollResp); err != nil {
			glog.V(2).Infof("decode body failed: %v", err)
			panic(err)
		}

		ids := e.getDeleteIds(scrollResp.Hits)
		outStream <- ids

		totalPage := scrollResp.Hits.Total / e.pageSize
		glog.V(2).Infof("totalPage: %d", totalPage)
		ctx := context.Background()
		//totalPage = 1
		for page := 0; page < totalPage; page++ {
			glog.V(3).Infof("streamGetIdsToDeleted Page-%d", page)
			select {
			case <-stop:
				ctx.Done()
				return
			default:
				res, err := e.c.Scroll(
					e.c.Scroll.WithContext(ctx),
					e.c.Scroll.WithScrollID(scrollResp.ScrollID),
					e.c.Scroll.WithScroll(10 * time.Minute))
				if err != nil {
					glog.V(2).Infof("scroll error: %v", err)
					return
				}
				var resp GenericResponse
				if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
					glog.V(2).Infof("decode error: %v", err)
					return
				}
				ids := e.getDeleteIds(resp.Hits)
				outStream <- ids
				res.Body.Close()
			}
		}
	}()

	return outStream
}

func (e *Esssss) Run(stop chan interface{}) {
	for iDate := e.dateStart; iDate <= e.dateEnd; iDate++ {
		date := strconv.Itoa(iDate)
		var num int
		start := time.Now()
		for obj := range e.streamDeleteIds(stop, e.streamGetIdsToDeleted(stop, date)) {
			num++
			glog.V(5).Infof("%v", obj)
		}
		glog.V(2).Infof("[%s] delete %d docs spend %v", date, num * e.pageSize, time.Since(start))
	}
}