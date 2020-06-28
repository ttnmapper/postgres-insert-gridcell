package main

import (
	"github.com/j4/gosm"
	"log"
	"sync"
	"time"
	"ttnmapper-postgres-insert-gridcell/types"
)

// Functions specific to this aggregation type

var (
	antennaDbCache  sync.Map
	gridCellDbCache sync.Map
)

func aggregateNewData(message types.TtnMapperUplinkMessage) {
	// Iterate gateways. We store it flat in the database
	for _, gateway := range message.Gateways {

		var antennaID uint = 0

		// We store coverage data per antenna, assuming antenna index 0 when we don't know the antenna index.
		antennaIndexer := types.AntennaIndexer{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId, AntennaIndex: gateway.AntennaIndex}
		i, ok := antennaDbCache.Load(antennaIndexer)
		if ok {
			antennaID = i.(uint)
		} else {
			antennaDb := types.Antenna{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId, AntennaIndex: gateway.AntennaIndex}
			err := db.FirstOrCreate(&antennaDb, &antennaDb).Error
			if err != nil {
				continue
			}
			antennaID = antennaDb.ID
			antennaDbCache.Store(antennaIndexer, antennaDb.ID)
		}

		seconds := message.Time / 1000000000
		nanos := message.Time % 1000000000
		entryTime := time.Unix(seconds, nanos)

		log.Println("AntennaID ", antennaID)
		incrementBucket(antennaID, message.Latitude, message.Longitude, entryTime, gateway.Rssi, gateway.Snr)
	}
}

func aggregateMovedGateway(movedGateway types.TtnMapperGatewayMoved) {

	seconds := movedGateway.Time / 1000000000
	nanos := movedGateway.Time % 1000000000
	movedTime := time.Unix(seconds, nanos)

	log.Println("Gateway ", movedGateway.GatewayId, "moved at ", movedTime)

	// Find the antenna IDs for the moved gateway
	var antennas []types.Antenna
	db.Where(&types.Antenna{NetworkId: movedGateway.NetworkId, GatewayId: movedGateway.GatewayId}).Find(&antennas)

	for _, antenna := range antennas {
		log.Println("AntennaID ", antenna.ID)

		// Get a list of grid cells to delete
		var gridCells []types.GridCell
		db.Where("antenna_id = ?", antenna.ID).Find(&gridCells)

		// Remove from local cache
		for _, gridCell := range gridCells {
			gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
			gridCellDbCache.Delete(gridCellIndexer)
		}
		// Then remove from sql
		db.Where(&types.GridCell{AntennaID: antenna.ID}).Delete(&types.GridCell{})

		// Get all existing packets since gateway last moved
		var packets []types.Packet
		db.Where("antenna_id = ? AND time > ?", antenna.ID, movedTime).Find(&packets)

		for _, packet := range packets {
			incrementBucket(antenna.ID, packet.Latitude, packet.Longitude, packet.Time, packet.Rssi, packet.Snr)
		}
	}

}

func incrementBucket(antennaId uint, latitude float64, longitude float64, time time.Time, rssi float32, snr float32) {

	tile := gosm.NewTileWithLatLong(latitude, longitude, 19)

	gridCellDb := types.GridCell{}

	// Try and find in cache first
	gridCellIndexer := types.GridCellIndexer{AntennaId: antennaId, X: tile.X, Y: tile.Y}
	i, ok := gridCellDbCache.Load(gridCellIndexer)
	if ok {
		gridCellDb = i.(types.GridCell)
		log.Println("Found grid cell in cache")
		log.Println(gridCellDb)
	} else {
		gridCellDb.AntennaID = antennaId
		gridCellDb.X = tile.X
		gridCellDb.Y = tile.Y
		err := db.FirstOrCreate(&gridCellDb, &gridCellDb).Error
		if err != nil {
			failOnError(err, "Failed to find db entry for grid cell")
		}
		log.Println("Found grid cell in db")
	}

	signal := rssi
	if snr < 0 {
		signal += snr
	}

	if signal > -95 {
		gridCellDb.BucketHigh++
	} else if signal > -100 {
		gridCellDb.Bucket100++
	} else if signal > -105 {
		gridCellDb.Bucket105++
	} else if signal > -110 {
		gridCellDb.Bucket110++
	} else if signal > -115 {
		gridCellDb.Bucket115++
	} else if signal > -120 {
		gridCellDb.Bucket120++
	} else if signal > -125 {
		gridCellDb.Bucket125++
	} else if signal > -130 {
		gridCellDb.Bucket130++
	} else if signal > -135 {
		gridCellDb.Bucket135++
	} else if signal > -140 {
		gridCellDb.Bucket140++
	} else if signal > -145 {
		gridCellDb.Bucket145++
	} else {
		gridCellDb.BucketLow++
	}

	if time.After(gridCellDb.LastUpdated) {
		gridCellDb.LastUpdated = time
	}

	// Save to db
	log.Println("Storing in DB")
	log.Println(gridCellDb)
	db.Save(&gridCellDb)

	gridCellDbCache.Store(gridCellIndexer, gridCellDb)
}
