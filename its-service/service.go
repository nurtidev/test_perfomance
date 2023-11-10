package its_service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	pb "mep4-default/proto"
	"mep4-default/utils"
	"strconv"
	"strings"
	"time"

	"mep4-default/database"
	"mep4-default/models"
	sensors_json "mep4-default/sensors-json"
)

const IS_DEBUG = 0

// Our micro service
type Service struct {
	pb.UnimplementedGreeterServer
}

func (s *Service) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello"}, nil
}

func (s *Service) GetSplashes(ctx context.Context, req *pb.GetSplashesRequest) (*pb.Mep4Response, error) {

	packet := new(pb.Mep4Response)
	splash := new(pb.Splash)

	from := time.Unix(req.From, 0).Format(DATE_LAYOUT)
	to := time.Unix(req.To, 0).Format(DATE_LAYOUT)

	query := `select
				id, pm25, toUnixTimestamp(timestamp_sub(HOUR, 6, time)) as date, location_id
				from default.sensors_mep4 where id = ` + req.SensorId + ` and time >= '` + from + `' and time <= '` + to + `'
				and pm25 >= 40
				order by time asc`
	rows, err := database.ClQuerySQL(query)

	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	} else {
		for rows.Next() {
			item := ParseItem(rows)
			// Make Packet Of Splashes
			if MakePacketOfSplashes(item, splash, packet) {
				splash = new(pb.Splash)
				splash.Items = append(splash.Items, item)
			}
		}

		if len(splash.Items) > 1 {
			packet.Splashes = append(packet.Splashes, splash)
		}

		result := SplitPacketCutNoices(packet)
		return result, nil
	}
}

const DATE_LAYOUT = "2006-01-02 15:04:05"
const UTC_OFFSET int64 = -6

var noice uint64 = 60 * 2 //2 min noice
var step uint64 = 6       //6 sec step

func (s *Service) Run() {

	if utils.Getenv("CITY_ID", "") == "2" { //На сервере Алматы текущая дата UTC 0 ...
		loc, err := time.LoadLocation("Asia/Almaty")
		if err != nil {
			fmt.Println(err)
			return
		}
		time.Local = loc
		year, month, day := time.Now().Date()
		today := time.Date(year, month, day, 0, 0, 0, 0, time.Now().Location())
		log.Printf("Setup time zone to current %v UTC - %v", today, today.UTC())
	}

	sensorsList := sensors_json.CamerasJson{}
	sensorsList.Init(utils.Getenv("CITY_ID", "2"))

	sensorList := []models.SensorMep4{}

	for _, item := range sensorsList.Meps {

		//disable 16
		if item.GetId() == "16" {
			sensorList = append(sensorList, models.SensorMep4{InstallDate: "", Enable: false, Sensor: item})
			continue
		}

		//offset of install time of 13
		if item.GetId() == "13" {
			sensorList = append(sensorList, models.SensorMep4{InstallDate: "2022-09-05 23:59:59", Enable: true, Sensor: item})
			continue
		}

		sensorList = append(sensorList, models.SensorMep4{InstallDate: "", Enable: true, Sensor: item})
	}

	for {

		year, month, day := time.Now().Date()
		sixHours := time.Date(year, month, day, 6, 0, 0, 0, time.Now().Location())

		if IsTimeToStart(sixHours) {
			// if true {

			fromDate, _ := time.Parse(DATE_LAYOUT, "2022-08-30 18:00:00")
			toDate := fromDate.Add(24 * time.Hour)
			today, _ := time.Parse(DATE_LAYOUT, time.Now().AddDate(0, 0, -1).Format("2006-01-02")+" 18:00:00")

			for !today.Equal(fromDate) {

				log.Printf("Start data analize from %s to %s", fromDate, toDate)

				// debugList := []models.SensorMep4{
				// 	models.SensorMep4{"", true, sensors_json.CamerasJsonData{"mep0012", 1, 780, 1.0, 1.0}},
				// }

				for _, mep4 := range sensorList {

					if mep4.Enable {

						if mep4.InstallDate == "" || (mep4.GetInstallTime() != nil && fromDate.After(*mep4.GetInstallTime())) {

							sensor_id := mep4.Sensor.GetId()

							if !IsDataAvail(fromDate, toDate, sensor_id) {

								packet := new(pb.Mep4Response)
								splash := new(pb.Splash)

								from := time.Unix(fromDate.Unix(), 0).Format(DATE_LAYOUT)
								to := time.Unix(toDate.Unix(), 0).Format(DATE_LAYOUT)
								sensorId, _ := strconv.Atoi(sensor_id)

								query := `select
									id, pm25, toUnixTimestamp(timestamp_sub(HOUR, 6, time)) as date, location_id
									from default.sensors_mep4 where id = ` + sensor_id + ` and time >= '` + from + `' and time <= '` + to + `'
									and pm25 >= 40
									order by time asc`
								rows, err := database.ClQuerySQL(query)

								if err != nil {
									log.Fatalf("%v", err)
								} else {
									for rows.Next() {
										item := ParseItem(rows)
										// Make Packet Of Splashes
										if MakePacketOfSplashes(item, splash, packet) {
											splash = new(pb.Splash)
											splash.Items = append(splash.Items, item)
										}
									}

									if len(splash.Items) > 1 {
										packet.Splashes = append(packet.Splashes, splash)
									}

									result := SplitPacketCutNoices(packet)

									if len(result.Splashes) > 0 {
										where := "where "
										var timeInervals []string
										for _, splash := range result.Splashes {
											from := time.Unix(int64(splash.Items[0].Time), 0).UTC()
											to := time.Unix(int64(splash.Items[1].Time), 0).UTC()
											timeInervals = append(timeInervals, "(time >= '"+from.Format(DATE_LAYOUT)+"' and time <= '"+to.Format(DATE_LAYOUT)+"')")
											//log.Printf("[] Ranges %s - %s", from.Format(DATE_LAYOUT), to.Format(DATE_LAYOUT))
										}
										where += "(" + strings.Join(timeInervals, " OR ") + ")"
										location := strconv.Itoa(int(result.Splashes[0].Items[0].LocationId))
										where += " and location_id=" + location + " order by time asc"
										query := "select car_number, toUnixTimestamp(time) as timestamp from event " + where

										events, err := database.ClQuerySQL(query)
										if err != nil {
											log.Printf("Error: %v", err)
											continue
										}

										var (
											carNumber string
											eventTime uint64
										)

										tx, stmt, err := database.ClPrepareTX(models.Mep4Default{}, "mep4_default_report")

										for _, splash := range result.Splashes {

											carInserted := make(map[string]string)

											if err != nil {
												log.Fatalf("Error: %s", err)
											}

											//last row from stupid rows.Next() method uff...
											if len(carNumber) > 0 {
												if eventTime >= splash.Items[0].Time && eventTime <= splash.Items[1].Time {
													carInserted[carNumber] = carNumber

													mep4Row := makeMep4Row(carNumber, splash, uint64(sensorId))
													err := addToStmt(mep4Row, stmt)
													if err != nil {
														log.Fatalf("[ERROR] Exec: %s", err)
													}
												}
											}

											for events.Next() {

												if err := events.Scan(&carNumber, &eventTime); err != nil {
													log.Printf("Error: %v", err)
													carNumber = ""
													continue
												}

												//If car number not inserted into report in current splash context
												if _, ok := carInserted[carNumber]; !ok {

													if eventTime >= splash.Items[0].Time && eventTime <= splash.Items[1].Time {
														carInserted[carNumber] = carNumber
														mep4Row := makeMep4Row(carNumber, splash, uint64(sensorId))
														err := addToStmt(mep4Row, stmt)

														//log.Printf("Insert new car record %s %v", carNumber, time.Unix(int64(eventTime), 0))
														if err != nil {
															log.Fatalf("[ERROR] Exec: %s", err)
														}
													} else {
														break
													}
												}
											}
										}

										if IS_DEBUG != 1 {
											err = tx.Commit()
											if err != nil {
												log.Fatalf("[ERROR] Insert rows into ClickHouse: %s", err)
											} else {
												// from := time.Unix(int64(splash[0].time), 0).UTC().Format(DATE_LAYOUT)
												// to := time.Unix(int64(splash[1].time), 0).UTC().Format(DATE_LAYOUT)
												// log.Println("insert day " + from + " " + to)
											}
											stmt.Close()
										} else {
											stmt.Close()
										}

									} else {
										log.Println("[" + sensor_id + "] No splashes by period")
									}
								}
							} else {
								//log.Println("Data avail +++")
							}
						}
					}
				}

				log.Printf("Done data analize from %s to %s", fromDate, toDate)

				fromDate = fromDate.Add(24 * time.Hour)
				toDate = toDate.Add(24 * time.Hour)
			}
			log.Println("DONE")
		}

		time.Sleep(time.Second * 3600)
	}

	log.Println("EXIT 0")
}

func SplitPacketCutNoices(packet *pb.Mep4Response) *pb.Mep4Response {
	rawResult := new(pb.Mep4Response)
	for _, splash := range packet.Splashes {
		if (splash.Items[len(splash.Items)-1].Time - splash.Items[0].Time) <= noice {
			var timeRange pb.Splash
			timeRange.Items = append(timeRange.Items, splash.Items[0])
			timeRange.Items = append(timeRange.Items, splash.Items[len(splash.Items)-1])
			rawResult.Splashes = append(rawResult.Splashes, &timeRange)
		} else {
			//to := time.Unix(int64(splash.Items[len(splash.Items)-1].Time), 0).UTC()
			//from := time.Unix(int64(splash.Items[1].Time), 0).UTC()
			//log.Printf("Noice %s %s", from, to)
			rawResult.Noices = append(rawResult.Noices, splash.Items[1])
			rawResult.Noices = append(rawResult.Noices, splash.Items[len(splash.Items)-1])
		}
	}

	result := new(pb.Mep4Response)
	//disable noices by all day
	if len(rawResult.Noices) > 1 {
		minNoice := rawResult.Noices[0].Time
		maxNoice := rawResult.Noices[len(rawResult.Noices)-1].Time
		// from := time.Unix(int64(minNoice), 0).UTC()
		// to := time.Unix(int64(maxNoice), 0).UTC()
		// log.Printf("[%d] Noice range is = %s %s", noices.Items[0].LocationId, from, to)
		for _, splash := range rawResult.Splashes {
			if splash.Items[0].Time < minNoice || splash.Items[0].Time > maxNoice {
				result.Splashes = append(result.Splashes, splash)
			}
		}
		result.Noices = rawResult.Noices
		return result
	} else {
		return rawResult
	}
}

func MakePacketOfSplashes(item *pb.Mep4, curSplash *pb.Splash, packet *pb.Mep4Response) bool {
	if len(curSplash.Items) == 0 {
		curSplash.Items = append(curSplash.Items, item)
	} else {
		if (item.Time - curSplash.Items[len(curSplash.Items)-1].Time) <= step {
			curSplash.Items = append(curSplash.Items, item)
		} else {
			if len(curSplash.Items) > 1 {
				packet.Splashes = append(packet.Splashes, curSplash)
				return true
			}
		}
	}
	return false
}

func IsDataAvail(fromDate time.Time, toDate time.Time, sensorId string) bool {
	from := fromDate.Format(DATE_LAYOUT)
	to := toDate.Format(DATE_LAYOUT)
	query := `select count(*) as count from mep4_default_report where splash_from >= '` + from + `' and splash_from <= '` + to + `' and rate = 1 and sensor_id=` + sensorId

	rows, err := database.ClQuerySQL(query)
	if err == nil {
		for rows.Next() {
			var count int64
			rows.Scan(&count)
			if count == 0 {
				return false
			}
		}
	} else {
		log.Fatalf("%v", err)
	}
	return true
}

func IsTimeToStart(sixMorning time.Time) bool {

	//return true

	if time.Now().After(sixMorning) {

		timestamp := sixMorning.Add(-6 * time.Hour)

		from := timestamp.AddDate(0, 0, -1)
		to := timestamp
		query := `select count(*) as count from mep4_default_report where rate = 1 and splash_from >= '` + from.UTC().Format(DATE_LAYOUT) +
			`' and splash_from <= '` + to.UTC().Format(DATE_LAYOUT) + `'`
		rows, err := database.ClQuerySQL(query)
		if err == nil {
			for rows.Next() {
				var count int64
				rows.Scan(&count)
				if count == 0 {
					return true
				}
				break
			}
		} else {
			log.Printf("Error %s", err)
		}
	}

	return false
}

func addToStmt(mep4Row models.Mep4Default, stmt *sql.Stmt) error {
	values := database.GetValueForInsert(mep4Row)
	_, err := stmt.Exec(values...)
	return err
}

func makeMep4Row(carNumber string, splash *pb.Splash, sensorId uint64) models.Mep4Default {

	from := time.Unix(int64(splash.Items[0].Time), 0).Format(DATE_LAYOUT)
	to := time.Unix(int64(splash.Items[1].Time), 0).Format(DATE_LAYOUT)

	return models.Mep4Default{
		CarNumber:  carNumber,
		LocationId: int64(splash.Items[0].LocationId),
		SensorId:   int64(sensorId),
		SplashFrom: from,
		SplashTo:   to,
		Rate:       1,
	}
}

func ParseItem(rows *sql.Rows) *pb.Mep4 {
	var item pb.Mep4
	err := rows.Scan(&item.Id, &item.Pm25, &item.Time, &item.LocationId)
	if err != nil {
		log.Printf("%v", err)
	}
	return &item
}
