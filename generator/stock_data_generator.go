package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"stock_aggregator/models"
	"sync"
	"time"
)

var Symbols = [...]string{"META", "APPL", "AMZN", "NFLX", "GOOG"}

func main() {
	startTime := time.Now()
	numOfRecords := 1000
	outFileName := "./stock_data/stock_exchange_data.csv"

	// buffered channel for putting generated data
	// which will be consumed to dump to csv file
	dataChan := make(chan models.StockData, 100)
	errChan := make(chan error, 1)

	// goroutine to write to file
	var wg sync.WaitGroup
	wg.Add(1)
	go writeToFile(outFileName, dataChan, errChan, &wg)

	generateStockData(numOfRecords, dataChan, errChan)

	// close the channel to signal no more data will be written
	close(dataChan)

	wg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("Generated %v samples in %v seconds\n", numOfRecords, elapsedTime.Seconds())
}

func generateStockData(numOfRecords int, dataChan chan<- models.StockData, errChan <-chan error) {
	fmt.Println("Initiating stock data generator...")

	// calculate past time based on numOfRecords
	minsInPast := numOfRecords / 2
	now := time.Now().UTC()
	pastTime := now.Add(-time.Duration(minsInPast) * time.Minute)
	period := now.Sub(pastTime)

	// source seed for random number generation
	source := rand.New(rand.NewSource(now.UnixNano()))

	for i := 0; i < numOfRecords; i++ {
		select {
		case err := <-errChan:
			fmt.Println(err.Error())
			return
		case dataChan <- getStockData(source, &now, &period):
		}
	}
}

func writeToFile(filename string, dataChan <-chan models.StockData, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Initiating file writer...")
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		errChan <- fmt.Errorf("could not open transaction log file: %s %w", filename, err)
		return
	}
	defer fp.Close()

	writer := csv.NewWriter(fp)
	writer.Comma = models.DELIMITER

	defer writer.Flush()

	if err := writer.Write(models.HEADER[:]); err != nil {
		errChan <- fmt.Errorf("error writing header to the file: %s", fp.Name())
		return
	}

	buffer := make([][]string, 0, 100)
	for data := range dataChan {
		record := []string{
			data.Timestamp,
			data.Symbol,
			fmt.Sprintf("%.2f", data.Price),
			fmt.Sprintf("%v", data.Volume),
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
}

func getStockData(source *rand.Rand, currentTime *time.Time, period *time.Duration) models.StockData {
	randomDuration := time.Duration(source.Int63n(period.Nanoseconds()))
	symbol := Symbols[source.Intn(len(Symbols))]
	price := source.Float64() * 1000     // Random price between 0 and 1000
	volume := uint(source.Intn(1000000)) // Random volume upto 1,000,000
	timestamp := currentTime.Add(-randomDuration * time.Nanosecond).Format(models.TIME_FMT)

	return models.StockData{
		Symbol:    symbol,
		Price:     price,
		Volume:    volume,
		Timestamp: timestamp,
	}
}
