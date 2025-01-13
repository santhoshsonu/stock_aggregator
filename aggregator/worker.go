package aggregator

import (
	"fmt"
	"stock_aggregator/models"
	"sync"
	"time"
)

type Aggregator struct {
	Interval  uint // Interval in Minutes
	InputChan chan models.StockData
	ErrorChan chan error
	Results   map[string]map[string]AggregateResult
}

type AggregateResult struct {
	Timestamp          time.Time
	Open               float64
	High               float64
	Low                float64
	Close              float64
	Volume             uint
	lastOpenTimestamp  time.Time
	lastCloseTimestamp time.Time
}

type Aggregate struct {
	Symbol    string
	Timestamp string
	Result    AggregateResult
}

var HEADER = [...]string{"Timestamp", "Symbol", "Open", "High", "Low", "Close", "Volume"}

func NewAggregator(interval uint, input chan models.StockData, errChan chan error) *Aggregator {
	return &Aggregator{
		Interval:  interval,
		InputChan: input,
		ErrorChan: errChan,
		Results:   make(map[string]map[string]AggregateResult),
	}
}

func (agg *Aggregator) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer fmt.Printf("[Aggregator-%dMin] Finished\n", agg.Interval)

	fmt.Printf("[Aggregator-%dMin] Starting...\n", agg.Interval)
	for {
		select {
		case <-agg.ErrorChan:
			return
		case data, ok := <-agg.InputChan:
			if !ok {
				return
			}
			// fmt.Printf("Received: %v\n", data)
			timestamp, err := time.Parse(models.TIME_FMT, data.Timestamp)
			if err != nil {
				fmt.Printf("[Aggregator-%dMin] Error: %v", agg.Interval, err)
				agg.ErrorChan <- fmt.Errorf("[Aggregator-%dMin] error: %v", agg.Interval, err)
				break
			}

			roundedTimestamp := getRoundedTimestamp(timestamp, agg.Interval)
			intervalKey := roundedTimestamp.Format(models.TIME_FMT)

			timestampResultMap, ok := agg.Results[data.Symbol]
			if !ok {
				fmt.Println("Initializing aggregation result for symbol: ", data.Symbol)
				// Make new map for symbol with intervalkey
				agg.Results[data.Symbol] = map[string]AggregateResult{
					intervalKey: {
						Timestamp:          roundedTimestamp,
						Open:               data.Price,
						High:               data.Price,
						Low:                data.Price,
						Close:              data.Price,
						Volume:             data.Volume,
						lastOpenTimestamp:  timestamp,
						lastCloseTimestamp: timestamp,
					},
				}
			} else {
				result, ok := timestampResultMap[intervalKey]
				if !ok {
					// fmt.Printf("Initializing aggregation result for symbol: %s and time: %v\n", data.Symbol, intervalKey)
					// create a new aggregate result for the intervalKey
					timestampResultMap[intervalKey] = AggregateResult{
						Timestamp: roundedTimestamp,
						Open:      data.Price,
						High:      data.Price,
						Low:       data.Price,
						Close:     data.Price,
						Volume:    data.Volume,
					}
				} else {
					// fmt.Printf("Updating aggregation result for symbol: %s and time: %v\n", data.Symbol, intervalKey)
					// update aggregate result for the intervalKey
					result.Volume += data.Volume
					if timestamp.Before(result.lastOpenTimestamp) {
						result.Open = data.Price
						result.lastOpenTimestamp = timestamp
					}
					if timestamp.After(result.lastCloseTimestamp) {
						result.Close = data.Price
						result.lastCloseTimestamp = timestamp
					}
					if data.Price > result.High {
						result.High = data.Price
					}
					if data.Price < result.Low {
						result.Low = data.Price
					}
					timestampResultMap[intervalKey] = result
				}
			}
		}
	}
}

func getRoundedTimestamp(timestamp time.Time, interval uint) time.Time {
	return timestamp.Truncate(time.Duration(interval) * time.Minute)
}
