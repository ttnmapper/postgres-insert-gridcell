package types

import "time"

type GridCell struct {
	ID        uint
	AntennaID uint `gorm:"UNIQUE_INDEX:idx_grid_cell"`

	X int `gorm:"UNIQUE_INDEX:idx_grid_cell"`
	Y int `gorm:"UNIQUE_INDEX:idx_grid_cell"`
	// Z is always 19

	LastUpdated time.Time

	BucketHigh     uint
	Bucket100      uint
	Bucket105      uint
	Bucket110      uint
	Bucket115      uint
	Bucket120      uint
	Bucket125      uint
	Bucket130      uint
	Bucket135      uint
	Bucket140      uint
	Bucket145      uint
	BucketLow      uint
	BucketNoSignal uint
}

type GridCellIndexer struct {
	AntennaId uint
	X         int
	Y         int
}
