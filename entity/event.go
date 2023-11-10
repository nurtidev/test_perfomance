package entity

import (
	pb "create-events/proto"
	"reflect"
)

type Event struct {
	CityId        uint32 `json:"city_id,omitempty"`
	Id            uint64 `json:"id,omitempty"`
	CarNumber     string `json:"car_number,omitempty"`
	LocationId    int64  `json:"location_id,omitempty"`
	LocationType  string `json:"location_type,omitempty"`
	CameraId      int64  `json:"camera_id,omitempty"`
	ViolationId   int64  `json:"violation_id,omitempty"`
	Time          uint64 `json:"time,omitempty"`
	Line          int32  `json:"line,omitempty"`
	Speed         int32  `json:"speed,omitempty"`
	SpeedLimit    int32  `json:"speed_limit,omitempty"`
	Probability   string `json:"probability,omitempty"`
	MaxSpeedCv    int32  `json:"max_speed_cv,omitempty"`
	DebugTracks   string `json:"debug_tracks,omitempty"`
	Picture       string `json:"picture,omitempty"`
	Host          string `json:"host,omitempty"`
	LpType        string `json:"lp_type,omitempty"`
	CarDirection  string `json:"car_direction,omitempty"`
	Scale         string `json:"scale,omitempty"`
	TimestampDiff string `json:"timestamp_diff,omitempty"`
	IsWrong       uint8  `json:"is_wrong,omitempty"`
}

// NewEventFromShortEvent get new Event and filled with values from generated.ShortEvent
func NewEventFromShortEvent(shortEvent *pb.ShortEvent) *Event {
	newEvent := Event{}
	shortEventValue := reflect.ValueOf(shortEvent).Elem()
	eventValue := reflect.ValueOf(&newEvent).Elem()
	for i := 0; i < shortEventValue.NumField(); i++ {
		shortEventValueField := shortEventValue.Field(i)
		shortEventTypeField := shortEventValue.Type().Field(i)
		shortEventFieldName := shortEventTypeField.Name

		// Avoid IsWrong field.
		// Because ClickHouse doesn't support boolean variable.
		// Except that you should use uint8.
		if shortEventFieldName == "IsWrong" {
			continue
		}

		eventValueField := eventValue.FieldByName(shortEventFieldName)
		if eventValueField.IsValid() && shortEventValueField.IsValid() {
			eventValueField.Set(shortEventValueField)
		}

	}
	return &newEvent
}
