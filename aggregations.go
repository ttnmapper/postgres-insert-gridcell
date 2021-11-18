package main

import (
	"errors"
	"fmt"
	"github.com/j4/gosm"
	"github.com/umahmood/haversine"
	"gorm.io/gorm/clause"
	"log"
	"sync"
	"time"
	"ttnmapper-postgres-insert-gridcell/types"
)

// Functions specific to this aggregation type

var (
	antennaDbCache  sync.Map
	gatewayDbCache  sync.Map
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

		// If the point is too far from the gateway, ignore it
		if !CheckDistanceFromGateway(gateway, message) {
			continue
		}

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

	// Delete gateway from gateway cache
	gatewayIndexer := types.GatewayIndexer{
		NetworkId: movedGateway.NetworkId,
		GatewayId: movedGateway.GatewayId,
	}
	gatewayDbCache.Delete(gatewayIndexer)

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
	rows, err := db.Model(&types.Packet{}).Where("antenna_id = ? AND time > ? AND experiment_id IS NULL", antenna.ID, installedAtLocation).Rows() // server side cursor
	if err != nil {
		log.Fatalf(err.Error())
	}

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

		// If the point is too far from the gateway, ignore it
		if !CheckDistanceFromAntenna(antenna, packet) {
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
	err = rows.Close()
	if err != nil {
		log.Println(err.Error())
	}

	// Delete old cells from database
	db.Where(&types.GridCell{AntennaID: antenna.ID}).Delete(&types.GridCell{})

	if len(gatewayGridCells) == 0 {
		log.Println("No packets")
		return
	}
	fmt.Println()

	// Then add new ones
	log.Printf("Result is %d grid cells", len(gatewayGridCells))
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

	gridCellsSlice := make([]types.GridCell, 0)
	for _, val := range gridCells {
		gridCellsSlice = append(gridCellsSlice, val)
	}

	// On conflict override
	tx := db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&gridCellsSlice)
	return tx.Error
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

func CheckDistanceFromAntenna(antenna types.Antenna, packet types.Packet) bool {

	gateway := types.TtnMapperGateway{NetworkId: antenna.NetworkId, GatewayId: antenna.GatewayId}
	message := types.TtnMapperUplinkMessage{Latitude: packet.Latitude, Longitude: packet.Longitude}

	return CheckDistanceFromGateway(gateway, message)
}

func CheckDistanceFromGateway(gateway types.TtnMapperGateway, message types.TtnMapperUplinkMessage) bool {
	var gatewayDb types.Gateway

	// Find the gateway so that we can check the distance of this point from the gateway
	gatewayIndexer := types.GatewayIndexer{
		NetworkId: gateway.NetworkId,
		GatewayId: gateway.GatewayId,
	}
	i, ok := gatewayDbCache.Load(gatewayIndexer)
	if ok {
		//log.Println("Gateway from cache")
		gatewayDb = i.(types.Gateway)
	} else {
		gatewayDb = types.Gateway{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId}
		//log.Println("Gateway from DB")
		err := db.First(&gatewayDb, &gatewayDb).Error
		if err != nil {
			log.Println(err.Error())
			return false // if we can't find the gateway, rather do not allow this point through
		}
		if gatewayDb.ID != 0 {
			gatewayDbCache.Store(gatewayIndexer, gatewayDb)
		}
	}

	gatewayLatitude := 0.0
	if gatewayDb.Latitude != nil {
		gatewayLatitude = *gatewayDb.Latitude
	}
	gatewayLongitude := 0.0
	if gatewayDb.Longitude != nil {
		gatewayLongitude = *gatewayDb.Longitude
	}

	if gatewayLatitude == 0 && gatewayLongitude == 0 {
		// Null island, exclude gateways with unknown locations
		return false
	} else {
		oldLocation := haversine.Coord{Lat: gatewayLatitude, Lon: gatewayLongitude}
		newLocation := haversine.Coord{Lat: message.Latitude, Lon: message.Longitude}
		_, km := haversine.Distance(oldLocation, newLocation)

		if km > myConfiguration.GatewayMaximumRangeKm {
			return false
		} else {
			return true
		}
	}
}
