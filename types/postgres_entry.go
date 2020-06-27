package types

import (
	"time"
)

type Packet struct {
	ID   uint
	Time time.Time `gorm:"not null;index=time"`

	DeviceID uint `gorm:"not null;index=device"`

	FPort uint8
	FCnt  uint32

	FrequencyID  uint
	DataRateID   uint
	CodingRateID uint

	// Gateway data
	AntennaID              uint `gorm:"not null;index=antenna"`
	GatewayTime            *time.Time
	Timestamp              *uint32
	FineTimestamp          *uint64
	FineTimestampEncrypted *[]byte
	FineTimestampKeyID     *uint
	ChannelIndex           uint32
	Rssi                   float32  `gorm:"type:numeric(6,2)"`
	SignalRssi             *float32 `gorm:"type:numeric(6,2)"`
	Snr                    float32  `gorm:"type:numeric(5,2)"`

	Latitude         float64  `gorm:"not null;type:numeric(10,6);index:latitude"`
	Longitude        float64  `gorm:"not null;type:numeric(10,6);index:longitude"`
	Altitude         float64  `gorm:"type:numeric(6,1)"`
	AccuracyMeters   *float64 `gorm:"type:numeric(6,2)"`
	Satellites       *int32
	Hdop             *float64 `gorm:"type:numeric(4,1)"`
	AccuracySourceID uint

	ExperimentID *uint

	UserID      uint
	UserAgentID uint

	DeletedAt *time.Time
}

type Device struct {
	ID      uint
	AppId   string `gorm:"UNIQUE_INDEX:app_device"`
	DevId   string `gorm:"UNIQUE_INDEX:app_device"`
	DevEui  string // EUI is like a description, and can change
	Packets []Packet
}

type Frequency struct {
	ID      uint
	Herz    uint64 `gorm:"unique;not null"`
	Packets []Packet
}

type DataRate struct {
	ID              uint
	Modulation      string `gorm:"UNIQUE_INDEX:data_rate"` // LORA or FSK or LORA-E
	Bandwidth       uint64 `gorm:"UNIQUE_INDEX:data_rate"`
	SpreadingFactor uint8  `gorm:"UNIQUE_INDEX:data_rate"`
	Bitrate         uint64 `gorm:"UNIQUE_INDEX:data_rate"`
	Packets         []Packet
}

type CodingRate struct {
	ID      uint
	Name    string `gorm:"unique;not null"`
	Packets []Packet
}

type AccuracySource struct {
	ID      uint
	Name    string `gorm:"unique;not null"`
	Packets []Packet
}

type Experiment struct {
	ID      uint
	Name    string `gorm:"unique;not null"`
	Packets []Packet
}

type User struct {
	ID         uint
	Identifier string `gorm:"unique;not null"`
	Packets    []Packet
}

type UserAgent struct {
	ID      uint
	Name    string `gorm:"unique;not null"`
	Packets []Packet
}

// TODO: Currently we identify a gateway using the gateway ID provided by the network.
// But how are we going to identify them between networks, when data is sent via the packet broker?

type Antenna struct {
	ID uint

	// TTN gateway ID along with the Antenna index identifies a unique coverage area.
	NetworkId    string `gorm:"type:varchar(36);UNIQUE_INDEX:gtw_id_antenna"`
	GatewayId    string `gorm:"type:varchar(36);UNIQUE_INDEX:gtw_id_antenna"`
	AntennaIndex uint8  `gorm:"UNIQUE_INDEX:gtw_id_antenna"`

	// For now we do not set antenna locations, but add it here for future use
	//Latitude         *float64
	//Longitude        *float64
	//Altitude         *int32

	Packets []Packet
}

type Gateway struct {
	ID uint

	NetworkId   string `gorm:"type:varchar(36);UNIQUE_INDEX:idx_gtw_id"`
	GatewayId   string `gorm:"type:varchar(36);UNIQUE_INDEX:idx_gtw_id"`
	GatewayEui  *string
	Description *string

	Latitude         float64
	Longitude        float64
	Altitude         int32
	LocationAccuracy *int32
	LocationSource   *string

	//AtLocationSince	time.Time // This value gets updated when the gateway moves
	LastHeard time.Time // This value always gets updated to reflect that the gateway is working

	Antennas         []Antenna
	GatewayLocations []GatewayLocation
}

type GatewayLocation struct {
	ID        uint
	NetworkId string `gorm:"type:varchar(36);INDEX=idx_gtw_id_install"`
	GatewayId string `gorm:"type:varchar(36);INDEX=idx_gtw_id_install"`

	InstalledAt time.Time `gorm:"INDEX=idx_gtw_id_install"`
	Latitude    float64
	Longitude   float64
}

// To blacklist a gateway set its location to 0,0
type GatewayLocationForce struct {
	ID        uint
	NetworkId string `gorm:"type:varchar(36);UNIQUE_INDEX:idx_gtw_id_force"`
	GatewayId string `gorm:"type:varchar(36);UNIQUE_INDEX:idx_gtw_id_force"`

	Latitude  float64
	Longitude float64
}

type FineTimestampKeyID struct {
	ID                          uint
	FineTimestampEncryptedKeyId string
}

// Indexers: These structs are the same as the ones above, but used to index the cache maps
type DeviceIndexer struct {
	DevId string
	AppId string
}

type GatewayIndexer struct {
	NetworkId string
	GatewayId string
}

type AntennaIndexer struct {
	NetworkId    string
	GatewayId    string
	AntennaIndex uint8
}

type DataRateIndexer struct {
	Modulation      string // LORA or FSK or LORA-E
	Bandwidth       uint64
	SpreadingFactor uint8
	Bitrate         uint64
}
