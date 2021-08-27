package main

import (
	"flag"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tkanos/gonfig"
	"log"
	"net/http"
	"time"
	"ttnmapper-postgres-insert-gridcell/types"
)

type Configuration struct {
	AmqpHost                 string `env:"AMQP_HOST"`
	AmqpPort                 string `env:"AMQP_PORT"`
	AmqpUser                 string `env:"AMQP_USER"`
	AmqpPassword             string `env:"AMQP_PASSWORD"`
	AmqpExchangeInsertedData string `env:"AMQP_EXCHANGE_INSERTED"`
	AmqpQueueInsertedData    string `env:"AMQP_QUEUE_INSERTED"`
	AmqpExchangeGatewayMoved string `env:"AMQP_EXCHANGE_GATEWAY_MOVED"`
	AmqpQueueGatewayMoved    string `env:"AMQP_QUEUE_GATEWAY_MOVED"`

	PostgresHost     string `env:"POSTGRES_HOST"`
	PostgresPort     string `env:"POSTGRES_PORT"`
	PostgresUser     string `env:"POSTGRES_USER"`
	PostgresPassword string `env:"POSTGRES_PASSWORD"`
	PostgresDatabase string `env:"POSTGRES_DATABASE"`
	PostgresDebugLog bool   `env:"POSTGRES_DEBUG_LOG"`

	PrometheusPort string `env:"PROMETHEUS_PORT"`
}

var myConfiguration = Configuration{
	AmqpHost:                 "localhost",
	AmqpPort:                 "5672",
	AmqpUser:                 "user",
	AmqpPassword:             "password",
	AmqpExchangeInsertedData: "inserted_data",
	AmqpQueueInsertedData:    "inserted_data_gridcell",
	AmqpExchangeGatewayMoved: "gateway_moved",
	AmqpQueueGatewayMoved:    "gateway_moved_gridcell",

	PostgresHost:     "localhost",
	PostgresPort:     "5432",
	PostgresUser:     "username",
	PostgresPassword: "password",
	PostgresDatabase: "database",
	PostgresDebugLog: false,

	PrometheusPort: "9100",
}

var (
	// Prometheus stats
	processedLive = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_gridcell_live_count",
		Help: "The total number of live messages processed",
	})
	processedMoved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_gridcell_moved_count",
		Help: "The total number of moved messages processed",
	})
	deletedGridCells = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_gridcell_deleted_count",
		Help: "The total number of grid cells deleted",
	})
	oldDataProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_gridcell_old_data_count",
		Help: "The total number of old data points processed to grid cells",
	})
	updatedGridCells = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_gridcell_updated_count",
		Help: "The total number of grid cells updated in database",
	})

	processLiveDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_gridcell_live_duration",
		Help:    "How long the processing and insert of a live message takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})
	processMovedDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_gridcell_moved_duration",
		Help:    "How long the processing and insert of a moved gateway takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})

	// Other global vars
	db *gorm.DB
)

func main() {

	reprocess := flag.Bool("reprocess", false, "a bool")
	flag.Parse()

	err := gonfig.GetConf("conf.json", &myConfiguration)
	if err != nil {
		log.Println(err)
	}

	log.Printf("[Configuration]\n%s\n", prettyPrint(myConfiguration)) // output: [UserA, UserB]

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe("0.0.0.0:"+myConfiguration.PrometheusPort, nil)
		if err != nil {
			log.Print(err.Error())
		}
	}()

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
	defer db.Close()

	if myConfiguration.PostgresDebugLog {
		db.LogMode(true)
	}

	// Create tables if they do not exist
	log.Println("Performing auto migrate")
	db.AutoMigrate(
		// TODO: add the tables this service is responsible for maintaining
		//&types.Gateway{},
		&types.GridCell{},
	)

	// Should we reprocess or listen for live data?
	if *reprocess {
		log.Println("Reprocessing")

		ReprocessSpiess()

	} else {
		// Start amqp listener threads
		log.Println("Starting AMQP thread")
		go subscribeToRabbitNewData()
		go subscribeToRabbitMovedGateway()

		// Starting processing threads
		go processNewData()
		go processMovedGateway()

		log.Printf("Init Complete")
		forever := make(chan bool)
		<-forever
	}

}

func ReprocessSpiess() {
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
	}
	rows.Close()
}
