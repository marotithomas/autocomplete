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
    ListenPort         = "80"
)

func mustGetenv(key string) string {
    val := os.Getenv(key)
    if val == "" {
        log.Fatalf("Missing required environment variable: %s", key)
    }
    return val
}

// SearchResult tartalmazza az autocomplete javaslatokat és a debug információkat.
type SearchResult struct {
    Suggestions []string `json:"suggestions"`
    Debug       string   `json:"debug,omitempty"`
}

// MappingCheckResult ad információt az index mapping ellenőrzéséről.
type MappingCheckResult struct {
    FieldMappingExists bool   `json:"fieldMappingExists"`
    UniqueCount        int    `json:"uniqueCount"`
    Debug              string `json:"debug,omitempty"`
}

// caseInsensitiveRegex generál egy reguláris kifejezést, amely az adott string minden karakterére
// létrehoz egy karakterosztályt, így például "sze" → "[sS][zZ][eE].*"
func caseInsensitiveRegex(query string) string {
    var sb strings.Builder
    for _, ch := range query {
        if ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') {
            lower := strings.ToLower(string(ch))
            upper := strings.ToUpper(string(ch))
            sb.WriteString("[" + lower + upper + "]")
        } else {
            sb.WriteRune(ch)
        }
    }
    sb.WriteString(".*")
    return sb.String()
}

// createIndex hozza létre az indexet a megfelelő mappinggel,
// ahol a "telepules" mezőhöz hozzáadjuk a "keyword" almezőt.
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
                        "filter": []string{
                            "lowercase",
                            "autocomplete_filter",
                        },
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
    client := &http.Client{}
    resp, err := client.Do(req)
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

// performOpenSearchAutocomplete aggregációs lekérdezést futtat a "telepules.keyword" mezőn,
// az include paraméterhez a caseInsensitiveRegex függvény által generált reguláris kifejezést használva.
// Így azokat az egyedi városneveket adja vissza, amelyek a felhasználó által beírt prefix-szel kezdődnek.
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
        debugBuffer.WriteString(fmt.Sprintf("Hiba a payload marshalolásakor: %v\n", err))
        return nil, debugBuffer.String(), err
    }
    debugBuffer.WriteString("Aggregation Payload JSON: " + string(payloadBytes) + "\n")

    url := fmt.Sprintf("%s/%s/_search", OpenSearchURL, IndexName)
    req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
    if err != nil {
        debugBuffer.WriteString(fmt.Sprintf("Hiba a HTTP kérés létrehozásakor: %v\n", err))
        return nil, debugBuffer.String(), err
    }
    req.Header.Set("Content-Type", "application/json")
    req.SetBasicAuth(OpenSearchUser, OpenSearchPassword)
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        debugBuffer.WriteString(fmt.Sprintf("Hiba az OpenSearch lekérdezés végrehajtásakor: %v\n", err))
        return nil, debugBuffer.String(), err
    }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        debugBuffer.WriteString(fmt.Sprintf("Hiba a válasz beolvasásakor: %v\n", err))
        return nil, debugBuffer.String(), err
    }
    debugBuffer.WriteString(fmt.Sprintf("OpenSearch válasz státusza: %d\n", resp.StatusCode))
    debugBuffer.WriteString("Válasz body: " + string(body) + "\n")

    var result map[string]interface{}
    if err := json.Unmarshal(body, &result); err != nil {
        debugBuffer.WriteString(fmt.Sprintf("Hiba a válasz JSON dekódolásakor: %v\n", err))
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
    debugBuffer.WriteString(fmt.Sprintf("Visszaadott javaslatok: %v\n", suggestions))
    return suggestions, debugBuffer.String(), nil
}

// autocompleteHandler kezeli az /api/autocomplete végpontot.
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
    if err := json.NewEncoder(w).Encode(response); err != nil {
        log.Printf("Hiba a válasz kódolásakor: %v", err)
    }
}

// checkMapping lekéri az index mappingjét, és aggregációs lekérdezéssel megszámolja az egyedi "telepules.keyword" értékeket.
func checkMapping() (MappingCheckResult, error) {
    var result MappingCheckResult
    var debugBuffer bytes.Buffer

    // Mapping lekérdezés
    mappingURL := fmt.Sprintf("%s/%s/_mapping", OpenSearchURL, IndexName)
    req, err := http.NewRequest("GET", mappingURL, nil)
    if err != nil {
        return result, err
    }
    req.SetBasicAuth(OpenSearchUser, OpenSearchPassword)
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return result, err
    }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return result, err
    }
    debugBuffer.WriteString("Mapping lekérdezés válasz body: " + string(body) + "\n")
    var mapping map[string]interface{}
    if err := json.Unmarshal(body, &mapping); err != nil {
        return result, err
    }
    fieldMappingExists := false
    if indexMapping, ok := mapping[IndexName].(map[string]interface{}); ok {
        if mappings, ok := indexMapping["mappings"].(map[string]interface{}); ok {
            if properties, ok := mappings["properties"].(map[string]interface{}); ok {
                if telepulesField, ok := properties["telepules"].(map[string]interface{}); ok {
                    if fields, ok := telepulesField["fields"].(map[string]interface{}); ok {
                        if _, ok := fields["keyword"]; ok {
                            fieldMappingExists = true
                        }
                    }
                }
            }
        }
    }
    result.FieldMappingExists = fieldMappingExists

    // Aggregáció a "telepules.keyword" egyedi értékeinek megszámolására
    aggQuery := map[string]interface{}{
        "size": 0,
        "aggs": map[string]interface{}{
            "unique_telepules": map[string]interface{}{
                "terms": map[string]interface{}{
                    "field": "telepules.keyword",
                    "size":  100,
                },
            },
        },
    }
    aggBytes, err := json.Marshal(aggQuery)
    if err != nil {
        return result, err
    }
    aggURL := fmt.Sprintf("%s/%s/_search", OpenSearchURL, IndexName)
    reqAgg, err := http.NewRequest("POST", aggURL, bytes.NewReader(aggBytes))
    if err != nil {
        return result, err
    }
    reqAgg.Header.Set("Content-Type", "application/json")
    reqAgg.SetBasicAuth(OpenSearchUser, OpenSearchPassword)
    respAgg, err := client.Do(reqAgg)
    if err != nil {
        return result, err
    }
    defer respAgg.Body.Close()
    aggBody, err := io.ReadAll(respAgg.Body)
    if err != nil {
        return result, err
    }
    debugBuffer.WriteString("Aggregáció válasz body: " + string(aggBody) + "\n")
    var aggResult map[string]interface{}
    if err := json.Unmarshal(aggBody, &aggResult); err != nil {
        return result, err
    }
    uniqueCount := 0
    if aggs, ok := aggResult["aggregations"].(map[string]interface{}); ok {
        if bucketAgg, ok := aggs["unique_telepules"].(map[string]interface{}); ok {
            if buckets, ok := bucketAgg["buckets"].([]interface{}); ok {
                uniqueCount = len(buckets)
            }
        }
    }
    result.UniqueCount = uniqueCount
    result.Debug = debugBuffer.String()
    return result, nil
}

// mappingCheckHandler kezeli az /api/checkMapping végpontot.
func mappingCheckHandler(w http.ResponseWriter, r *http.Request) {
    res, err := checkMapping()
    if err != nil {
        http.Error(w, "Hiba a mapping ellenőrzésekor", http.StatusInternalServerError)
        log.Printf("Mapping check error: %v", err)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(res); err != nil {
        log.Printf("Hiba a mapping check válasz kódolásakor: %v", err)
    }
}

// demoHandler szolgáltatja a demo HTML felületet.
func demoHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    html := `
<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Buddha's Autocomplete Demo</title>
<style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    input { width: 300px; padding: 8px; font-size: 1em; }
    button { margin-top: 10px; padding: 8px 12px; font-size: 1em; }
    ul {
        list-style: none;
        padding: 0;
        margin-top: 10px;
        width: 300px;
    }
    li {
        padding: 5px 10px;
    }
    li:hover {
        background-color: #e0e0e0;
        cursor: pointer;
        border-radius: 4px;
    }
    #error { color: red; margin-top: 10px; }
    #debug { margin-top: 20px; white-space: pre-wrap; background: #f0f0f0; padding: 10px; border: 1px solid #ccc; }
    #validationResult { margin-top: 10px; font-weight: bold; }
</style>
</head>
<body>
<h1>Buddha's Autocomplete Demo</h1>
<input type="text" id="autocomplete" placeholder="Kezdj el gépelni egy települést...">
<button id="validateBtn">Validáció</button>
<ul id="suggestions"></ul>
<div id="error"></div>
<h2>Debug:</h2>
<div id="debug"></div>
<div id="validationResult"></div>
<script>
let currentSuggestions = [];
const input = document.getElementById('autocomplete');
const suggestionsList = document.getElementById('suggestions');
const errorDiv = document.getElementById('error');
const debugDiv = document.getElementById('debug');
const validateBtn = document.getElementById('validateBtn');
const validationResult = document.getElementById('validationResult');

input.addEventListener('input', () => {
    const query = input.value;
    errorDiv.textContent = "";
    debugDiv.textContent = "";
    validationResult.textContent = "";
    if(query.length < 2) {
        suggestionsList.innerHTML = '';
        currentSuggestions = [];
        return;
    }
    fetch('/api/autocomplete?q=' + encodeURIComponent(query))
        .then(response => {
            if(!response.ok) throw new Error("HTTP hiba: " + response.status);
            return response.json();
        })
        .then(data => {
            suggestionsList.innerHTML = '';
            currentSuggestions = data.suggestions;
            data.suggestions.forEach(item => {
                const li = document.createElement('li');
                li.textContent = item;
                li.addEventListener('click', () => {
                    input.value = item;
                    suggestionsList.innerHTML = '';
                    validationResult.textContent = "";
                });
                suggestionsList.appendChild(li);
            });
            debugDiv.textContent = data.debug;
        })
        .catch(err => {
            errorDiv.textContent = "Hiba történt: " + err.message;
        });
});

validateBtn.addEventListener('click', () => {
    const inputVal = input.value.trim();
    if(inputVal === "") {
        validationResult.textContent = "Az input üres!";
        validationResult.style.color = "red";
        return;
    }
    const isValid = currentSuggestions.includes(inputVal);
    validationResult.textContent = isValid ? "Az input érvényes." : "Az input nem egyezik az adatbázissal.";
    validationResult.style.color = isValid ? "green" : "red";
});
</script>
</body>
</html>
`
    fmt.Fprint(w, html)
}

func main() {
    OpenSearchHost = mustGetenv("OPENSEARCH_HOST")
    OpenSearchPort = mustGetenv("OPENSEARCH_PORT")
    OpenSearchUser = mustGetenv("OPENSEARCH_USER")
    OpenSearchPassword = mustGetenv("OPENSEARCH_PASSWORD")
    OpenSearchURL = fmt.Sprintf("https://%s:%s", OpenSearchHost, OpenSearchPort)

    http.HandleFunc("/api/autocomplete", autocompleteHandler)
    http.HandleFunc("/api/checkMapping", mappingCheckHandler)
    http.HandleFunc("/", demoHandler)

    port := os.Getenv("PORT")
    if port == "" {
        port = ListenPort
    }
    addr := fmt.Sprintf(":%s", port)
    log.Printf("Server listening on port %s", port)
    if err := http.ListenAndServe(addr, nil); err != nil {
        log.Fatal("Server error:", err)
    }
}

