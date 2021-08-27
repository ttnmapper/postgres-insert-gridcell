package main

import (
	"errors"
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

	processedLive.Inc()

	// Iterate gateways. We store it flat in the database
	for _, gateway := range message.Gateways {
		gatewayStart := time.Now()

		var antennaID uint = 0

		// We store coverage data per antenna, assuming antenna index 0 when we don't know the antenna index.
		antennaIndexer := types.AntennaIndexer{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId, AntennaIndex: gateway.AntennaIndex}
		i, ok := antennaDbCache.Load(antennaIndexer)
		if ok {
			log.Println("Antenna from cache", antennaIndexer)
			antennaID = i.(uint)
		} else {
			antennaDb := types.Antenna{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId, AntennaIndex: gateway.AntennaIndex}
			log.Println("Antenna from db", antennaDb)
			err := db.FirstOrCreate(&antennaDb, &antennaDb).Error
			if err != nil {
				log.Println(err.Error())
				continue
			}
			antennaID = antennaDb.ID
			antennaDbCache.Store(antennaIndexer, antennaDb.ID)
		}

		seconds := message.Time / 1000000000
		nanos := message.Time % 1000000000
		entryTime := time.Unix(seconds, nanos)

		log.Print("AntennaID ", antennaID)
		gridCell, err := getGridCell(antennaID, message.Latitude, message.Longitude)
		if err != nil {
			continue
		}
		incrementBucket(&gridCell, entryTime, gateway.Rssi, gateway.Snr)
		StoreGridCellInCache(gridCell)
		StoreGridCellInDb(gridCell)

		// Prometheus stats
		gatewayElapsed := time.Since(gatewayStart)
		processLiveDuration.Observe(float64(gatewayElapsed.Nanoseconds()) / 1000.0 / 1000.0) //nanoseconds to milliseconds
	}
}

func aggregateMovedGateway(movedGateway types.TtnMapperGatewayMoved) {

	processedMoved.Inc()

	seconds := movedGateway.Time / 1000000000
	nanos := movedGateway.Time % 1000000000
	movedTime := time.Unix(seconds, nanos)

	log.Print("Gateway ", movedGateway.GatewayId, "moved at ", movedTime)

	// Find the antenna IDs for the moved gateway
	var antennas []types.Antenna
	db.Where(&types.Antenna{NetworkId: movedGateway.NetworkId, GatewayId: movedGateway.GatewayId}).Find(&antennas)

	for _, antenna := range antennas {
		ReprocessAntenna(antenna, movedTime)
	}

}

func ReprocessAntenna(antenna types.Antenna, installedAtLocation time.Time) {
	antennaStart := time.Now()

	log.Print("AntennaID ", antenna.ID)

	// Get a list of grid cells to delete
	var gridCells []types.GridCell
	db.Where("antenna_id = ?", antenna.ID).Find(&gridCells)

	// Remove from local cache
	for _, gridCell := range gridCells {
		deletedGridCells.Inc()
		gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
		gridCellDbCache.Delete(gridCellIndexer)
	}
	// Then remove from sql
	db.Where(&types.GridCell{AntennaID: antenna.ID}).Delete(&types.GridCell{})

	// Get all existing packets since gateway last moved
	var packets []types.Packet
	db.Where("antenna_id = ? AND time > ?", antenna.ID, installedAtLocation).Find(&packets)

	gatewayGridCells := sync.Map{}
	for _, packet := range packets {
		oldDataProcessed.Inc()
		gridCell, err := getGridCell(antenna.ID, packet.Latitude, packet.Longitude)
		if err != nil {
			continue
		}
		incrementBucket(&gridCell, packet.Time, packet.Rssi, packet.Snr)
		StoreGridCellInCache(gridCell)
		StoreGridCellInTempCache(&gatewayGridCells, gridCell)
	}

	gatewayGridCells.Range(func(key, value interface{}) bool {
		gridCell, ok := value.(types.GridCell)
		if !ok {
			return true
		}
		StoreGridCellInDb(gridCell)
		return true
	})

	// Prometheus stats
	antennaElapsed := time.Since(antennaStart)
	processMovedDuration.Observe(float64(antennaElapsed.Nanoseconds()) / 1000.0 / 1000.0) //nanoseconds to milliseconds
}

func getGridCell(antennaId uint, latitude float64, longitude float64) (types.GridCell, error) {
	// https://blog.jochentopf.com/2013-02-04-antarctica-in-openstreetmap.html
	// The Mercator projection generally used in online maps only covers the area between about 85.0511 degrees South and 85.0511 degrees North.
	if latitude < -85 || latitude > 85 {
		// We get a tile index that is invalid if we try handling -90,-180
		return types.GridCell{}, errors.New("coordinates out of range")
	}

	tile := gosm.NewTileWithLatLong(latitude, longitude, 19)

	gridCellDb := types.GridCell{}

	// Try and find in cache first
	gridCellIndexer := types.GridCellIndexer{AntennaId: antennaId, X: tile.X, Y: tile.Y}
	i, ok := gridCellDbCache.Load(gridCellIndexer)
	if ok {
		gridCellDb = i.(types.GridCell)
		log.Print("Found grid cell in cache")
	} else {
		gridCellDb.AntennaID = antennaId
		gridCellDb.X = tile.X
		gridCellDb.Y = tile.Y
		err := db.FirstOrCreate(&gridCellDb, &gridCellDb).Error
		if err != nil {
			log.Print(antennaId, latitude, longitude, tile.X, tile.Y)
			failOnError(err, "Failed to find db entry for grid cell")
		}
		log.Print("Found grid cell in db")
	}
	return gridCellDb, nil
}

func StoreGridCellInCache(gridCell types.GridCell) {
	// Save to cache
	gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
	gridCellDbCache.Store(gridCellIndexer, gridCell)
}

func StoreGridCellInTempCache(tempCache *sync.Map, gridCell types.GridCell) {
	// Save to cache
	gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
	tempCache.Store(gridCellIndexer, gridCell)
}

func StoreGridCellInDb(gridCell types.GridCell) {
	// Save to db
	log.Println("Storing in DB")
	//log.Println(gridCellDb)
	db.Save(&gridCell)
}

func incrementBucket(gridCell *types.GridCell, time time.Time, rssi float32, snr float32) {
	signal := rssi
	if snr < 0 {
		signal += snr
	}

	if signal > -95 {
		gridCell.BucketHigh++
	} else if signal > -100 {
		gridCell.Bucket100++
	} else if signal > -105 {
		gridCell.Bucket105++
	} else if signal > -110 {
		gridCell.Bucket110++
	} else if signal > -115 {
		gridCell.Bucket115++
	} else if signal > -120 {
		gridCell.Bucket120++
	} else if signal > -125 {
		gridCell.Bucket125++
	} else if signal > -130 {
		gridCell.Bucket130++
	} else if signal > -135 {
		gridCell.Bucket135++
	} else if signal > -140 {
		gridCell.Bucket140++
	} else if signal > -145 {
		gridCell.Bucket145++
	} else {
		gridCell.BucketLow++
	}

	if time.After(gridCell.LastUpdated) {
		gridCell.LastUpdated = time
	}

	updatedGridCells.Inc()
}
