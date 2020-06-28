package main

import (
	"encoding/json"
	"ttnmapper-postgres-insert-gridcell/types"
)

func processNewData() {
	// Process new data messages from channel
	go func() {
		for data := range newDataChannel {
			// The message form amqp is a json string. Unmarshal to ttnmapper uplink struct
			var message types.TtnMapperUplinkMessage
			if err := json.Unmarshal(data.Body, &message); err != nil {
				continue
			}

			// This aggregation does not use experiment data
			if message.Experiment != "" {
				continue
			}

			aggregateNewData(message)
		}
	}()
}

func processMovedGateway() {
	// Process moved gateway messages from channel
	go func() {
		for data := range gatewayMovedChannel {
			// The message form amqp is a json string. Unmarshal to ttnmapper moved gateway struct
			var message types.TtnMapperGatewayMoved
			if err := json.Unmarshal(data.Body, &message); err != nil {
				continue
			}

			aggregateMovedGateway(message)
		}
	}()
}
