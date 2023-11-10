package sensors_json

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"strconv"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type CamerasJson struct {
	Meps       []CamerasJsonData
	sensorsMap map[string]CamerasJsonData
}

type CamerasJsonData struct {
	Id         string  `json:"id"`
	CameraId   int64   `json:"camera_id","omitempty"`
	LocationId int64   `json:"location_id","omitempty"`
	Lat        float64 `json:"lat","omitempty"`
	Lon        float64 `json:"lon","omitempty"`
}

func (c *CamerasJson) Init(cityId string) {
	data, err := ioutil.ReadFile("./sensors-json/city_cameras_" + cityId + ".json")
	check(err)
	var result CamerasJson
	err = json.Unmarshal(data, &result)
	check(err)
	c.Meps = result.Meps
	c.sensorsMap = make(map[string]CamerasJsonData)
	for _, item := range result.Meps {
		c.sensorsMap[item.Id] = item
	}
}

func (c *CamerasJsonData) GetId() string {
	id := c.Id[3:len(c.Id)]
	Id, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Printf("parse mep4 id err %v", err)
		return ""
	}
	return strconv.Itoa(int(Id))
}

func (c *CamerasJson) GetCamera(mepId string) (error, *CamerasJsonData) {
	if val, ok := c.sensorsMap[mepId]; ok {
		return nil, &val
	}
	return errors.New("Mep sensors ID " + mepId + " not defined in JSON."), nil
}
