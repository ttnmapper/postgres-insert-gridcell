package main

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tkanos/gonfig"
	"log"
	"net/http"
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
	processedData = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_gridcell_updated_count",
		Help: "The total number of gridcell updates that are made",
	})

	// Other global vars
	db *gorm.DB
)

func main() {

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

	// Start amqp listener threads
	log.Println("Starting AMQP thread")
	subscribeToRabbitNewData()
	subscribeToRabbitMovedGateway()

	// Starting processing threads
	processNewData()
	processMovedGateway()

	log.Printf("Init Complete")
	forever := make(chan bool)
	<-forever

}
