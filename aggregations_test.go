package main

import (
	"github.com/jinzhu/gorm"
	"github.com/tkanos/gonfig"
	"log"
	"testing"
	"time"
	"ttnmapper-postgres-insert-gridcell/types"
)

func IniDb() {
	err := gonfig.GetConf("conf.json", &myConfiguration)
	if err != nil {
		log.Println(err)
	}

	//log.Printf("[Configuration]\n%s\n", prettyPrint(myConfiguration)) // output: [UserA, UserB]

	// Table name prefixes
	gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
		//return "ttnmapper_" + defaultTableName
		return defaultTableName
	}

	var dbErr error
	// pq: unsupported sslmode "prefer"; only "require" (default), "verify-full", "verify-ca", and "disable" supported - so we disable it
	db, dbErr = gorm.Open("postgres", "host="+myConfiguration.PostgresHost+" port="+myConfiguration.PostgresPort+" user="+myConfiguration.PostgresUser+" dbname="+myConfiguration.PostgresDatabase+" password="+myConfiguration.PostgresPassword+" sslmode=disable")
	if dbErr != nil {
		log.Println("Error connecting to Postgres")
		panic(dbErr.Error())
	}
	//defer db.Close()

	if myConfiguration.PostgresDebugLog {
		db.LogMode(true)
	}
}

func TestAggregateMovedGateway(t *testing.T) {
	IniDb()

	//movedGateway := types.TtnMapperGatewayMoved{
	//	NetworkId:    "NS_TTS_V3://ttn@000013",
	//	GatewayId:    "eui-000080029c09dd87",
	//}
	movedGateway := types.TtnMapperGatewayMoved{
		NetworkId: "thethingsnetwork.org",
		GatewayId: "eui-58a0cbfffe8023e7",
	}
	aggregateMovedGateway(movedGateway)
	//gateway := types.Gateway{NetworkId: "thethingsnetwork.org", GatewayId: "eui-58a0cbfffe8023e7"}
	//ReprocessSingleGateway(gateway)
}

func TestReprocessSpiess(t *testing.T) {
	IniDb()

	// Get all gateways heard by device ID
	query := `
SELECT DISTINCT(antenna_id) FROM packets p
JOIN devices d on d.id = p.device_id
WHERE d.dev_id = 't-beam-tracker'
AND d.app_id = 'ttn-tracker-sensorsiot'`

	rows, _ := db.Raw(query).Rows()
	for rows.Next() {
		var antennaId uint
		rows.Scan(&antennaId)

		// Find the antenna IDs for the moved gateway
		var antenna types.Antenna
		db.First(&antenna, antennaId)

		var movedTime time.Time
		lastMovedQuery := `
SELECT max(installed_at) FROM gateway_locations
WHERE network_id = ?
AND gateway_id = ?`
		timeRow := db.Raw(lastMovedQuery, antenna.NetworkId, antenna.GatewayId).Row()
		timeRow.Scan(&movedTime)

		log.Println(antenna.GatewayId, movedTime)
		ReprocessAntenna(antenna, movedTime)
		break
	}
	rows.Close()
	db.Close()
}

func TestReprocessHelium(t *testing.T) {
	IniDb()

	var antennas []types.Antenna
	db.Where("network_id = ?", "NS_HELIUM://000024").Find(&antennas)

	for _, antenna := range antennas {
		var movedTime time.Time
		lastMovedQuery := `
SELECT max(installed_at) FROM gateway_locations
WHERE network_id = ?
AND gateway_id = ?`
		timeRow := db.Raw(lastMovedQuery, antenna.NetworkId, antenna.GatewayId).Row()
		timeRow.Scan(&movedTime)

		log.Println(antenna.GatewayId, movedTime)
		ReprocessAntenna(antenna, movedTime)
	}
	db.Close()
}
