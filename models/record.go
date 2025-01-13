package models

type StockData struct {
	Timestamp string
	Symbol    string
	Price     float64
	Volume    uint
}

const DELIMITER rune = ';'
const TIME_FMT = "2006-01-02T15:04:05Z"

var HEADER = [...]string{"Timestamp", "Symbol", "Price", "Volume"}
