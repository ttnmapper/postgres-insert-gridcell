package main

import (
	"errors"
	"fmt"
	"github.com/j4/gosm"
	"log"
	"strings"
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

	if message.Experiment != "" {
		return
	}
	if message.Latitude == 0 && message.Longitude == 0 {
		return
	}

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

	//seconds := movedGateway.Time / 1000000000
	//nanos := movedGateway.Time % 1000000000
	//movedTime := time.Unix(seconds, nanos)

	var movedTime time.Time
	lastMovedQuery := `
SELECT max(installed_at) FROM gateway_locations
WHERE gateway_id = ?`
	timeRow := db.Raw(lastMovedQuery, movedGateway.GatewayId).Row()
	timeRow.Scan(&movedTime)
	log.Print("Gateway ", movedGateway.GatewayId, "moved at ", movedTime)

	// Find the antenna IDs for the moved gateway
	var antennas []types.Antenna
	if movedGateway.NetworkId == "thethingsnetwork.org" {
		db.Where(&types.Antenna{GatewayId: movedGateway.GatewayId}).Find(&antennas)
	} else {
		db.Where(&types.Antenna{NetworkId: movedGateway.NetworkId, GatewayId: movedGateway.GatewayId}).Find(&antennas)
	}

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

	gatewayGridCells := map[types.GridCellIndexer]types.GridCell{}

	// Get all existing packets since gateway last moved
	//db.Where("antenna_id = ? AND time > ? AND experiment_id IS NULL", antenna.ID, installedAtLocation).Find(&packets) // uses too much memory
	rows, err := db.Model(&types.Packet{}).Where("antenna_id = ? AND time > ? AND experiment_id IS NULL", antenna.ID, installedAtLocation).Rows() // server side cursor
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		i++
		oldDataProcessed.Inc()
		fmt.Printf("\rPacket %d   ", i)

		var packet types.Packet
		err := db.ScanRows(rows, &packet)
		if err != nil {
			log.Println(err.Error())
			continue
		}

		gridCell, err := getGridCellNotDb(antenna.ID, packet.Latitude, packet.Longitude) // Do not create now as we will do a batch insert later
		if err != nil {
			continue
		}
		incrementBucket(&gridCell, packet.Time, packet.Rssi, packet.Snr)
		StoreGridCellInCache(gridCell) // so that we don't read it again from the database

		// Also store in a map of gridcells we will write to the database later
		gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
		gatewayGridCells[gridCellIndexer] = gridCell
	}

	// Delete old cells
	db.Where(&types.GridCell{AntennaID: antenna.ID}).Delete(&types.GridCell{})

	if len(gatewayGridCells) == 0 {
		log.Println("No packets")
		return
	}
	fmt.Println()

	// Then add new ones
	err = StoreGridCellsInDb(gatewayGridCells)
	if err != nil {
		log.Fatalf(err.Error())
	}

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
	if latitude == 0 && longitude == 0 {
		// We get a tile index that is invalid if we try handling -90,-180
		return types.GridCell{}, errors.New("null island")
	}

	tile := gosm.NewTileWithLatLong(latitude, longitude, 19)

	gridCellDb := types.GridCell{}

	// Try and find in cache first
	gridCellIndexer := types.GridCellIndexer{AntennaId: antennaId, X: tile.X, Y: tile.Y}
	i, ok := gridCellDbCache.Load(gridCellIndexer)
	if ok {
		gridCellDb = i.(types.GridCell)
		//log.Print("Found grid cell in cache")
	} else {
		gridCellDb.AntennaID = antennaId
		gridCellDb.X = tile.X
		gridCellDb.Y = tile.Y
		err := db.FirstOrCreate(&gridCellDb, &gridCellDb).Error
		if err != nil {
			log.Print(antennaId, latitude, longitude, tile.X, tile.Y)
			failOnError(err, "Failed to find db entry for grid cell")
		}
		//log.Print("Found grid cell in db")
	}
	return gridCellDb, nil
}

func getGridCellNotDb(antennaId uint, latitude float64, longitude float64) (types.GridCell, error) {
	// https://blog.jochentopf.com/2013-02-04-antarctica-in-openstreetmap.html
	// The Mercator projection generally used in online maps only covers the area between about 85.0511 degrees South and 85.0511 degrees North.
	if latitude < -85 || latitude > 85 {
		// We get a tile index that is invalid if we try handling -90,-180
		return types.GridCell{}, errors.New("coordinates out of range")
	}
	if latitude == 0 && longitude == 0 {
		// We get a tile index that is invalid if we try handling -90,-180
		return types.GridCell{}, errors.New("null island")
	}

	tile := gosm.NewTileWithLatLong(latitude, longitude, 19)

	gridCell := types.GridCell{}

	// Try and find in cache
	gridCellIndexer := types.GridCellIndexer{AntennaId: antennaId, X: tile.X, Y: tile.Y}
	i, ok := gridCellDbCache.Load(gridCellIndexer)
	if ok {
		gridCell = i.(types.GridCell)
		//log.Print("Found grid cell in cache")
	} else {
		gridCell.AntennaID = antennaId
		gridCell.X = tile.X
		gridCell.Y = tile.Y
	}
	return gridCell, nil
}

func StoreGridCellInCache(gridCell types.GridCell) {
	// Save to cache
	gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
	gridCellDbCache.Store(gridCellIndexer, gridCell)
}

//func StoreGridCellInTempCache(tempCache *sync.Map, gridCell types.GridCell) {
//	// Save to cache
//	gridCellIndexer := types.GridCellIndexer{AntennaId: gridCell.AntennaID, X: gridCell.X, Y: gridCell.Y}
//	tempCache.Store(gridCellIndexer, gridCell)
//}

func StoreGridCellInDb(gridCell types.GridCell) {
	// Save to db
	//log.Println("Storing in DB")
	//log.Println(gridCellDb)
	db.Save(&gridCell)
}

func StoreGridCellsInDb(gridCells map[types.GridCellIndexer]types.GridCell) error {
	if len(gridCells) == 0 {
		log.Println("No grid cells to insert")
		return nil
	}

	tx := db.Begin()
	var valueStrings []string
	var valueArgs []interface{}
	fields := "antenna_id,x,y,last_updated,bucket_high,bucket100,bucket105,bucket110,bucket115,bucket120,bucket125," +
		"bucket130,bucket135,bucket140,bucket145,bucket_low,bucket_no_signal"

	for _, gridCell := range gridCells {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, gridCell.AntennaID)
		valueArgs = append(valueArgs, gridCell.X)
		valueArgs = append(valueArgs, gridCell.Y)
		valueArgs = append(valueArgs, gridCell.LastUpdated)
		valueArgs = append(valueArgs, gridCell.BucketHigh)
		valueArgs = append(valueArgs, gridCell.Bucket100)
		valueArgs = append(valueArgs, gridCell.Bucket105)
		valueArgs = append(valueArgs, gridCell.Bucket110)
		valueArgs = append(valueArgs, gridCell.Bucket115)
		valueArgs = append(valueArgs, gridCell.Bucket120)
		valueArgs = append(valueArgs, gridCell.Bucket125)
		valueArgs = append(valueArgs, gridCell.Bucket130)
		valueArgs = append(valueArgs, gridCell.Bucket135)
		valueArgs = append(valueArgs, gridCell.Bucket140)
		valueArgs = append(valueArgs, gridCell.Bucket145)
		valueArgs = append(valueArgs, gridCell.BucketLow)
		valueArgs = append(valueArgs, gridCell.BucketNoSignal)

		// Insert in batches of 1000 to prevent too large query error
		// pq: got 104686 parameters but PostgreSQL only supports 65535 parameters
		if len(valueStrings) > 1000 { // 17 fields x 1000 = 17000 field parameters
			stmt := fmt.Sprintf("INSERT INTO grid_cells (%s) VALUES %s", fields, strings.Join(valueStrings, ","))
			err := tx.Exec(stmt, valueArgs...).Error
			if err != nil {
				tx.Rollback()
				return err
			}

			valueStrings = make([]string, 0)
			valueArgs = make([]interface{}, 0)
		}
	}

	if len(valueStrings) == 0 {
		log.Println("nothing in last batch")
		return nil
	}

	// We can do an insert here rather than an update, because the existing grid cells have been deleted during reprocessing before we get here
	stmt := fmt.Sprintf("INSERT INTO grid_cells (%s) VALUES %s", fields, strings.Join(valueStrings, ","))
	err := tx.Exec(stmt, valueArgs...).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit().Error
	//tx.Rollback()
	return err
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
