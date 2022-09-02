package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
)

func analyze(text string, es *elasticsearch.Client) {
	var b strings.Builder
	tmpl := `
    "analyzer": "ik_smart",
    "text": "%v"
    `
	b.WriteString("{\n")
	b.WriteString(fmt.Sprintf(tmpl, text))
	b.WriteString("\n}")

	// fmt.Printf("body: %s\n", b.String())

	req := es.Indices.Analyze.WithBody(strings.NewReader(b.String()))
	resp, err := es.Indices.Analyze(req)
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()

	if resp.IsError() {
		data, _ := io.ReadAll(resp.Body)
		log.Fatalln(string(data))
	}

	type AnalyzeResponse struct {
		Tokens []struct {
			Token       string
			StartOffset int `json:"start_offset"`
			EndOffset   int `json:"end_offset"`
			Type        string
			Position    int
		}
	}

	ar := AnalyzeResponse{}

	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("%+v\n", ar)
}

func aggregate(es *elasticsearch.Client) {
	var b strings.Builder
	tmpl := `
		"size": 0,
		"aggs": {
			"avg_price": {
				"avg": {
					"field": "price"
				}
			},
			"total_price": {
				"sum": {
					"field": "price"
				}
			}
		}
    `
	b.WriteString("{\n")
	b.WriteString(tmpl)
	b.WriteString("\n}")

	resp, err := es.Search(es.Search.WithIndex("hotel"), es.Search.WithBody(strings.NewReader(b.String())))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		data, _ := io.ReadAll(resp.Body)
		log.Fatalln(string(data))
	}

	type AggregateResult struct {
		// Took     int
		// TimedOut bool        `json:"timed_out"`
		// Shards   interface{} `json:"_shards"`
		// Hits     interface{} `json:"hits"`
		Data struct {
			TotalPrice struct {
				Value float64
			} `json:"total_price"`
			AvgPrice struct {
				Value float64
			} `json:"avg_price"`
		} `json:"aggregations"`
	}

	var res = AggregateResult{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("%+v\n", res.Data)
}

func agg_city_room_avg_price(es *elasticsearch.Client) {
	q := `
	{
		"size": 0,
		"aggs": {
		  "group_city": {
			"terms": {
			  "field": "city"
			},
			"aggs": {
			  "by_room": {
				"terms": {
				  "field": "full_room"
				},
				"aggs": {
				  "avg_price": {
					"avg": {
					  "field": "price",
					  "missing": 200
					}
				  }
				}
			  }
			}
		  }
		}
	}
	`
	resp, err := es.Search(es.Search.WithIndex("hotel"), es.Search.WithBody(strings.NewReader(q)))
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()

	type AggregateResult struct {
		Aggregations struct {
			GroupCity struct {
				Buckets []struct {
					Key      string
					DocCount int `json:"doc_count"`
					ByRoom   struct {
						Buckets []struct {
							Full     int `json:"key"`
							AvgPrice struct {
								Value float64
							} `json:"avg_price"`
						}
					} `json:"by_room"`
				}
			} `json:"group_city"`
		}
	}

	var data AggregateResult
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("%+v\n", data)
	fmt.Printf("%s:%d:%v\n",
		data.Aggregations.GroupCity.Buckets[0].Key,
		data.Aggregations.GroupCity.Buckets[0].ByRoom.Buckets[0].Full,
		data.Aggregations.GroupCity.Buckets[0].ByRoom.Buckets[0].AvgPrice.Value)
}

func main() {
	// docker compose -f elastic.yml  cp es1:/usr/share/elasticsearch/config/certs/http_ca.crt .
	cert, err := ioutil.ReadFile("./http_ca.crt")
	if err != nil {
		log.Fatalf("ReadFile: %v\n", err)
	}

	cfg := elasticsearch.Config{
		Addresses: []string{"https://localhost:9200"},
		Username:  "elastic",
		Password:  "VqEUnEgvHW*TvA0Rxq3T",
		CACert:    cert,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("NewDefaultClient: %v\n", err)
	}

	esinfo, err := es.Info()
	if err != nil || esinfo.IsError() {
		log.Fatalf("Info: %v\n", err)
	}

	var info map[string]interface{}
	if err := json.NewDecoder(esinfo.Body).Decode(&info); err != nil {
		log.Fatalf("Decode: %v\n", err)
	}

	log.Printf("elastic %v\n", elasticsearch.Version)

	for k, v := range info {
		log.Printf("%s = %v\n", k, v)
	}

	// analyze("因而也就消除了自己的解决之道", es)
	// aggregate(es)
	agg_city_room_avg_price(es)
}
