package aggregator

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"stock_aggregator/models"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ProcessCSVFile(filename string, wg *sync.WaitGroup) (chan models.StockData, chan error) {
	inputChan := make(chan models.StockData, 16)
	errChan := make(chan error, 1)

	file, err := os.Open(filename)
	if err != nil {
		errChan <- err
		return inputChan, errChan
	}

	reader := csv.NewReader(file)
	reader.Comma = models.DELIMITER

	go func() {
		defer wg.Done()
		defer file.Close()
		defer fmt.Printf("Finished reading file: %s\n", filename)

		_, err := reader.Read()
		if err != nil {
			fmt.Printf("error reading header: %v\n", err)
			errChan <- err
			return
		}
		for {
			select {
			case err := <-errChan:
				fmt.Println(err)
				return
			default:
				row, err := reader.Read()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						fmt.Printf("error reading row: %v", err)
						errChan <- err
					}
					close(inputChan)
					return
				}
				data, err := parseRow(row)
				// skip invalid rows
				if err != nil {
					fmt.Printf("error parsing row: %s\n", err)
					continue
				}
				// fmt.Println("sending: ", data)
				inputChan <- data
			}
		}
	}()

	return inputChan, errChan
}

func parseRow(row []string) (models.StockData, error) {
	stockData := models.StockData{}
	if len(row) < len(models.HEADER) {
		return stockData, fmt.Errorf("too few values for the row: %v", row)
	}
	timestamp := strings.TrimSpace(row[0])
	if _, err := time.Parse(models.TIME_FMT, timestamp); err != nil {
		fmt.Printf("Error parsing timestamp: %s %v\n", timestamp, err)
		return stockData, err
	}
	price, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
	if err != nil {
		return stockData, err
	}
	volume, err := strconv.ParseUint(strings.TrimSpace(row[3]), 10, 64)
	if err != nil {
		return stockData, err
	}
	stockData.Timestamp = timestamp
	stockData.Symbol = strings.TrimSpace(row[1])
	stockData.Price = price
	stockData.Volume = uint(volume)

	return stockData, nil
}
