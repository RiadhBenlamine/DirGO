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
	"sync"
	"time"

	"github.com/urfave/cli/v2"
)
var re = regexp.MustCompile(`(?i)<title>(.*?)</title>`)

type Config struct {
	pathFile  string
	baseURL   string
	matchCode int
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

func brute(path string, results chan<- Result, client *http.Client, matchCode int) {
	response, err := client.Get(path)
	title := ""
	statusCode := 0
	if err != nil {
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(response.Body)
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return
	}
	body := string(bodyBytes)

	match := re.FindStringSubmatch(body)
	if match != nil {
		title = match[1]
	}
	statusCode = response.StatusCode
	if statusCode == matchCode {
		res := Result{
			path:       path,
			statusCode: statusCode,
			title:      title,
		}
		results <- res
	}

}

func worker(jobs <-chan string, results chan<- Result, wg *sync.WaitGroup, client *http.Client, matchCode int) {
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
			&cli.IntFlag{
				Name:        "match-code",
				Aliases:     []string{"m"},
				Value:       200,
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
	client := &http.Client{
		Timeout: time.Second * time.Duration(config.timeout),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.tlsCheck,
			},
		},
	}

	for range config.threads {
		wg.Add(1)
		go worker(jobs, results, &wg, client, config.matchCode)
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
