package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"stock_aggregator/aggregator"
	"stock_aggregator/models"
	"strings"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	intervals := []int{1, 5}
	filepath := "./stock_data/stock_exchange_data.csv"

	wg.Add(1)
	inputChan, errChan := aggregator.ProcessCSVFile(filepath, &wg)

	aggregators := make([]*aggregator.Aggregator, len(intervals))
	for ix, interval := range intervals {
		wg.Add(1)
		ch := make(chan models.StockData, 16)
		agg := aggregator.NewAggregator(uint(interval), ch, errChan)
		go agg.Run(&wg)
		aggregators[ix] = agg
	}

	// goroutine to fan out data from file reader to aggregators
	wg.Add(1)
	go func() {
		defer wg.Done()

		for data := range inputChan {
			for _, agg := range aggregators {
				agg.InputChan <- data
			}
		}

		for _, agg := range aggregators {
			close(agg.InputChan)
		}
	}()

	wg.Wait()

	// Write results to file
	for _, agg := range aggregators {
		wg.Add(1)
		fmt.Printf("[Aggregator - %dMin] ", agg.Interval)
		trimmedPath, _ := strings.CutSuffix(filepath, ".csv")
		outFileName := trimmedPath + fmt.Sprintf("_%d_Min.csv", agg.Interval)

		d := make(chan aggregator.Aggregate, 10)
		e := make(chan error, 1)
		go func() {
			defer wg.Done()

			for symbol, result := range agg.Results {
				// Extract the intervals from the map
				sortedIntervals := make([]string, 0, len(result))
				for interval := range result {
					sortedIntervals = append(sortedIntervals, interval)
				}

				// Sort the intervals
				sort.Strings(sortedIntervals)

				for _, interval := range sortedIntervals {
					d <- aggregator.Aggregate{
						Symbol:    symbol,
						Timestamp: interval,
						Result:    result[interval],
					}
				}
			}
			close(d)
		}()

		wg.Add(1)
		go writeToFile(outFileName, d, e, &wg)
	}

	wg.Wait()
}

func writeToFile(filename string, dataChan <-chan aggregator.Aggregate, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Initiating file writer...")
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		errChan <- fmt.Errorf("could not open file: %s %w", filename, err)
		return
	}
	defer fp.Close()

	writer := csv.NewWriter(fp)
	writer.Comma = models.DELIMITER

	defer writer.Flush()

	if err := writer.Write(aggregator.HEADER[:]); err != nil {
		errChan <- fmt.Errorf("error writing header to the file: %s", fp.Name())
		return
	}

	buffer := make([][]string, 0, 100)
	for data := range dataChan {
		record := []string{
			data.Timestamp,
			data.Symbol,
			fmt.Sprintf("%.2f", data.Result.Open),
			fmt.Sprintf("%.2f", data.Result.High),
			fmt.Sprintf("%.2f", data.Result.Low),
			fmt.Sprintf("%.2f", data.Result.Close),
			fmt.Sprintf("%v", data.Result.Volume),
		}

		buffer = append(buffer, record)
		if len(buffer) == cap(buffer) {
			err = writer.WriteAll(buffer)
			// Clear the buffer
			buffer = buffer[:0]
		}

		if err != nil {
			fmt.Println(err)
			errChan <- fmt.Errorf("error while writing stock data to file: %s %w", fp.Name(), err)
			close(errChan)
			return
		}
	}
	err = writer.WriteAll(buffer)
	if err != nil {
		fmt.Println(err)
		errChan <- fmt.Errorf("error while writing stock data to file: %s %w", fp.Name(), err)
	}

}
