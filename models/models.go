package models

import (
	"log"
	sensors_json "mep4-default/sensors-json"
	"time"
)

// type Mep4 struct {
// 	Id          int
// 	Pm25        int
// 	Time        uint64
// 	Location_id int
// }

// Dataabase model
type Mep4Default struct {
	CarNumber  string `json:"car_number,omitempty"`
	LocationId int64  `json:"location_id,omitempty"`
	Rate       int8   `json:"rate,omitempty"`
	SensorId   int64  `json:"sensor_id,omitempty"`
	SplashFrom string `json:"splash_from,omitempty"`
	SplashTo   string `json:"splash_to,omitempty"`
}

// SensorsModel
type SensorMep4 struct {
	InstallDate string
	Enable      bool
	Sensor      sensors_json.CamerasJsonData
}

func (c *SensorMep4) GetInstallTime() *time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", c.InstallDate)
	if err != nil {
		log.Printf("[Error] %v", err)
		return nil
	}
	return &t
}
