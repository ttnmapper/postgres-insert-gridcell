package main

import (
	"encoding/json"
	"ttnmapper-postgres-insert-gridcell/types"
)

// A new live packet came in. Add it to the appropriate gridcell.
func processNewData() {
	for data := range newDataChannel {
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
}

// If a gateway moved, delete and rebuild all its gridcells
func processMovedGateway() {
	for data := range gatewayMovedChannel {
		var message types.TtnMapperGatewayMoved
		if err := json.Unmarshal(data.Body, &message); err != nil {
			continue
		}

		aggregateMovedGateway(message)
	}
}
