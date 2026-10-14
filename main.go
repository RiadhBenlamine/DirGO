package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
)

var re = regexp.MustCompile(`(?i)<title>(.*?)</title>`)

type Config struct {
	pathFile  string
	baseURL   string
	matchCode string
	timeout   int
	threads   int
	tlsCheck  bool
	outfile   string
}

type Result struct {
	path       string
	statusCode int
	title      string
}

func loadWordlist(pathFile string) ([]string, error) {

	file, err := os.Open(pathFile)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	var words []string
	for scanner.Scan() {
		words = append(words, scanner.Text())
	}
	return words, err
}

func output(path string, results []Result) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)

	writer := bufio.NewWriter(file)

	for _, result := range results {
		_, err := fmt.Fprintf(
			writer,
			"%s - [%d] - [%s]\n",
			result.path,
			result.statusCode,
			result.title,
		)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

func brute(path string, results chan<- Result, client *http.Client, matchCodes map[int]bool) {
	response, err := client.Get(path)
	if err != nil {
		log.Printf("Request failed for %s: %v", path, err)
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(response.Body)

	// If wildcard marker 0 is set, accept every status code.
	if !matchCodes[0] && !matchCodes[response.StatusCode] {
		return
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Printf("Failed to read response body for %s: %v", path, err)
		return
	}

	title := ""
	matches := re.FindSubmatch(body)

	if len(matches) > 1 {
		title = strings.TrimSpace(string(matches[1]))
	}

	results <- Result{
		path:       path,
		statusCode: response.StatusCode,
		title:      title,
	}
}

func handleCodes(codes string) (map[int]bool, error) {
	result := make(map[int]bool)


	if strings.TrimSpace(codes) == "" {
		result[0] = true
		return result, nil
	}

	for code := range strings.SplitSeq(codes, ",") {
		code = strings.TrimSpace(code)

		if code == "" {
			return nil, fmt.Errorf("empty status code in input")
		}

		n, err := strconv.Atoi(code)
		if err != nil {
			return nil, fmt.Errorf("invalid status code %q: %w", code, err)
		}

		if n < 100 || n > 599 {
			return nil, fmt.Errorf("invalid HTTP status code: %d", n)
		}

		result[n] = true
	}

	return result, nil
}

func worker(jobs <-chan string, results chan<- Result, wg *sync.WaitGroup, client *http.Client, matchCode map[int]bool) {
	defer wg.Done()
	for path := range jobs {
		brute(path, results, client, matchCode)
	}
}

func main() {
	config := &Config{}
	cmd := &cli.App{
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "wordlist",
				Aliases:     []string{"w"},
				Required:    true,
				Usage:       "Path to wordlist",
				Destination: &config.pathFile,
			},
			&cli.IntFlag{
				Name:        "threads",
				Aliases:     []string{"t"},
				Value:       10,
				Usage:       "Number of threads",
				Destination: &config.threads,
			},
			&cli.StringFlag{
				Name:        "match-code",
				Aliases:     []string{"m"},
				Value:       "",
				Usage:       "Match code",
				Destination: &config.matchCode,
			},
			&cli.IntFlag{
				Name:        "timeout",
				Aliases:     []string{"T"},
				Value:       30,
				Usage:       "Timeout",
				Destination: &config.timeout,
			},
			&cli.StringFlag{
				Name:        "url",
				Aliases:     []string{"u"},
				Value:       "",
				Usage:       "URL: http(s)://example.com/",
				Destination: &config.baseURL,
			},
			&cli.BoolFlag{
				Name:        "disable-tls",
				Aliases:     []string{"k"},
				Value:       false,
				Usage:       "disable TLS check",
				Destination: &config.tlsCheck,
			},
			&cli.PathFlag{
				Name:        "outfile",
				Aliases:     []string{"o"},
				Destination: &config.outfile,
				Usage:       "Output file",
			},
		},
		Action: func(c *cli.Context) error {
			return nil
		},
	}
	if err := cmd.Run(os.Args); err != nil {
		log.Fatal(err)
	}

	listPath := config.pathFile
	wordlist, _ := loadWordlist(listPath)
	jobs := make(chan string)
	results := make(chan Result)
	var wg sync.WaitGroup
	baseURL := config.baseURL
	if baseURL == "" {
		log.Fatal("Base URL not specified")
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		// Default https
		baseURL = "https://" + baseURL
	}
	client := &http.Client{
		Timeout: time.Second * time.Duration(config.timeout),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.tlsCheck,
			},
		},
	}
	matchCodes, err := handleCodes(config.matchCode)
	if err != nil {
		log.Fatal(err)
	}
	for range config.threads {
		wg.Add(1)
		go worker(jobs, results, &wg, client, matchCodes)
	}
	go func() {
		defer close(jobs)
		for _, word := range wordlist {
			jobs <- baseURL + word
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	var found []Result

	for result := range results {
		fmt.Println(result.title, result.statusCode, result.path)
		found = append(found, result)
	}
	if config.outfile != "" {
		if err := output(config.outfile, found); err != nil {
			log.Fatal(err)
		}
	}
}
