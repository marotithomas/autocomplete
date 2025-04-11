package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	OpenSearchHost     string
	OpenSearchPort     string
	OpenSearchUser     string
	OpenSearchPassword string
	OpenSearchURL      string
	IndexName          = "orszagos_cimlista"
	ListenPort         = "8080"
)

func mustGetenv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Missing required environment variable: %s", key)
	}
	return val
}

type SearchResult struct {
	Suggestions []string `json:"suggestions"`
	Debug       string   `json:"debug,omitempty"`
}

type MappingCheckResult struct {
	FieldMappingExists bool   `json:"fieldMappingExists"`
	UniqueCount        int    `json:"uniqueCount"`
	Debug              string `json:"debug,omitempty"`
}

func caseInsensitiveRegex(query string) string {
	var sb strings.Builder
	for _, ch := range query {
		if ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') {
			sb.WriteString(fmt.Sprintf("[%c%c]", ch&^0x20, ch|0x20))
		} else {
			sb.WriteRune(ch)
		}
	}
	sb.WriteString(".*")
	return sb.String()
}

func createIndex() {
	fmt.Println("Új index létrehozása autocomplete beállításokkal...")
	payload := map[string]interface{}{
		"settings": map[string]interface{}{
			"analysis": map[string]interface{}{
				"filter": map[string]interface{}{
					"autocomplete_filter": map[string]interface{}{
						"type":     "edge_ngram",
						"min_gram": 1,
						"max_gram": 20,
					},
				},
				"analyzer": map[string]interface{}{
					"autocomplete": map[string]interface{}{
						"type":      "custom",
						"tokenizer": "standard",
						"filter":    []string{"lowercase", "autocomplete_filter"},
					},
				},
			},
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"telepules": map[string]interface{}{
					"type":            "text",
					"analyzer":        "autocomplete",
					"search_analyzer": "standard",
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{
							"type": "keyword",
						},
					},
				},
				"kozter_nev": map[string]interface{}{
					"type": "text",
				},
			},
		},
	}
	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/%s", OpenSearchURL, IndexName)
	req, err := http.NewRequest("PUT", url, bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Hiba a HTTP kérés létrehozásakor: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(OpenSearchUser, OpenSearchPassword)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Hiba az index létrehozásakor: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		fmt.Println("Az index sikeresen létrejött.")
	} else {
		fmt.Printf("Hiba az index létrehozása során: %s\n", string(respBody))
	}
	fmt.Println()
}

func performOpenSearchAutocomplete(query string) ([]string, string, error) {
	var debugBuffer bytes.Buffer
	debugBuffer.WriteString(fmt.Sprintf("Keresési lekérdezés (aggregation): %q\n", query))

	regexPattern := caseInsensitiveRegex(query)
	debugBuffer.WriteString(fmt.Sprintf("Generált regexp: %q\n", regexPattern))

	aggQuery := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"unique_telepules": map[string]interface{}{
				"terms": map[string]interface{}{
					"field":   "telepules.keyword",
					"include": regexPattern,
					"size":    10,
				},
			},
		},
	}
	payloadBytes, err := json.Marshal(aggQuery)
	if err != nil {
		return nil, debugBuffer.String(), err
	}
	url := fmt.Sprintf("%s/%s/_search", OpenSearchURL, IndexName)
	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, debugBuffer.String(), err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(OpenSearchUser, OpenSearchPassword)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, debugBuffer.String(), err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, debugBuffer.String(), err
	}
	debugBuffer.WriteString(fmt.Sprintf("OpenSearch válasz státusza: %d\n", resp.StatusCode))
	debugBuffer.WriteString("Válasz body: " + string(body) + "\n")

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		debugBuffer.WriteString("Hiba JSON dekódoláskor: " + err.Error())
		return nil, debugBuffer.String(), err
	}

	suggestions := []string{}
	if aggs, ok := result["aggregations"].(map[string]interface{}); ok {
		if bucketAgg, ok := aggs["unique_telepules"].(map[string]interface{}); ok {
			if buckets, ok := bucketAgg["buckets"].([]interface{}); ok {
				for _, bucket := range buckets {
					if b, ok := bucket.(map[string]interface{}); ok {
						if key, ok := b["key"].(string); ok {
							suggestions = append(suggestions, key)
						}
					}
				}
			}
		}
	}
	debugBuffer.WriteString(fmt.Sprintf("Javaslatok: %v\n", suggestions))
	return suggestions, debugBuffer.String(), nil
}

func autocompleteHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Hiányzó 'q' paraméter", http.StatusBadRequest)
		return
	}
	suggestions, debugInfo, err := performOpenSearchAutocomplete(query)
	if err != nil {
		http.Error(w, "Hiba a javaslatok lekérésekor", http.StatusInternalServerError)
		log.Printf("Autocomplete error: %v", err)
		return
	}
	response := SearchResult{Suggestions: suggestions, Debug: debugInfo}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func main() {
	OpenSearchHost = mustGetenv("OPENSEARCH_HOST")
	OpenSearchPort = mustGetenv("OPENSEARCH_PORT")
	OpenSearchUser = mustGetenv("OPENSEARCH_USER")
	OpenSearchPassword = mustGetenv("OPENSEARCH_PASSWORD")
	OpenSearchURL = fmt.Sprintf("http://%s:%s", OpenSearchHost, OpenSearchPort)

	http.HandleFunc("/api/autocomplete", autocompleteHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = ListenPort
	}
	addr := fmt.Sprintf(":%s", port)
	log.Printf("Szerver elindult a(z) %s porton", port)
	log.Fatal(http.ListenAndServe(addr, nil))
}
