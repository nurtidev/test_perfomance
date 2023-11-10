package database

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

// type ClickhouseClient *sql.DB

var (
	// ClickhouseClient store Clickhouse connection
	ClickhouseClient *sql.DB
	// ClickhouseHost is URL path to Clickhouse
	ClickhouseHost string
	// to check if connection clClosed
	clClosed bool
	// to check if error while checking connection to Clickhouse
	isClError              bool
	firstTimeConnectTime   time.Duration = 5
	defaultReconnectClTime time.Duration = 10
	reconnectClTime        time.Duration = 10
	reconnectClTryCount                  = 6 * defaultReconnectClTime
)

// ClInit create connection to Clickhouse
func ClInit(host string) error {
	if ClickhouseClient != nil || clClosed {
		return errors.New("[CLICKHOUSE][CONNECTION]: Connection already has started")
	}
	var err error
	ClickhouseHost = host
	// Wait to connect to ClickHouse
	time.Sleep(firstTimeConnectTime * time.Second)
	ClickhouseClient, err = connect()
	if err != nil {
		return err
	}
	go clickhouseReconnect()
	return nil
}

func connect() (*sql.DB, error) {
	cclient, err := sql.Open("clickhouse", ClickhouseHost)
	if err != nil {
		return nil, err
	}
	if err := cclient.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil, err
	}
	return cclient, nil
}

func clickhouseReconnect() {
	for {
		if clClosed || reconnectClTime > reconnectClTryCount {
			log.Println("[CLICKHOUSE][CONNECTION]: Reconnect attempts has ended")
			return
		}
		err := ClickhouseClient.Ping()
		if err != nil {
			isClError = true
			log.Println("[CLICKHOUSE][CONNECTION]: Connection failed. Trying to reconnect...:", err.Error())
			ClickhouseClient, err = sql.Open("clickhouse", ClickhouseHost)
			if err != nil {
				log.Println("[CLICKHOUSE][CONNECTION]: Error while reconnecting: ", err.Error())
			}
			// Check connection via ping
			err = ClickhouseClient.Ping()
			if err == nil {
				successClReconnect()
			}
		} else {
			if isClError {
				successClReconnect()
			}
		}
		time.Sleep(reconnectClTime * time.Second)
		if isClError {
			reconnectClTime += reconnectClTime
		}
	}
}

func successClReconnect() {
	log.Println("[CLICKHOUSE][CONNECTION]: Reconnected successfully")
	reconnectClTime = defaultReconnectClTime
	isClError = false
}

// ClClose close Clickhouse connection
func ClClose() {
	if ClickhouseClient != nil || clClosed {
		return
	}
	err := ClickhouseClient.Close()
	if err != nil {
		log.Println("Error while closing connection to Clickhouse: ", err.Error())
	}
	clClosed = true
}

// ClExecSQL execute query to Clickhouse withour returning any rows
func ClExecSQL(query string) error {
	_, err := ClickhouseClient.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

// ClQuerySQL execute query to Clickhouse that return rows
func ClQuerySQL(query string) (*sql.Rows, error) {
	rows, err := ClickhouseClient.Query(query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ClExecSQL2 execute query to Clickhouse withour returning any rows.
// Made it to avoid error "clickhouse: wrong placeholder count"
//
//	which relate to question mark "?" in sql.Exec(query).
func ClExecSQL2(query string) error {
	resp, err := http.Post(ClickhouseHost, "application/octet-stream", strings.NewReader(query))
	if err != nil {
		return fmt.Errorf("Error with post: %s", err.Error())
	}
	defer resp.Body.Close()

	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Error with read response body: %s", err.Error())
	}
	if resp.StatusCode != 200 && len(responseBody) > 0 {
		return fmt.Errorf("Error save result: Response body - %s", string(responseBody))
	}
	return nil
}

func GetTagValuesAsString(i interface{}) []string {
	elem := reflect.ValueOf(i)
	typeOfT := elem.Type()

	var tagValues []string
	for i := 0; i < elem.NumField(); i++ {
		tagVal := typeOfT.Field(i).Tag.Get("json")
		tagVal = strings.Split(tagVal, ",")[0]
		tagValues = append(tagValues, tagVal)
	}
	return tagValues
}

func GetValueForInsert(i interface{}) []interface{} {
	elem := reflect.ValueOf(i)

	var values []interface{}
	for i := 0; i < elem.NumField(); i++ {
		f := elem.Field(i)
		values = append(values, f.Interface())
	}

	return values
}

func ClPrepareTX(model interface{}, tableName string) (*sql.Tx, *sql.Stmt, error) {
	colNames := GetTagValuesAsString(model)
	var questionMarks []string
	for range colNames {
		questionMarks = append(questionMarks, "?")
	}

	tx, err := ClickhouseClient.Begin()
	if err != nil {
		return nil, nil, fmt.Errorf("Begin: %s", err)
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ","),
		strings.Join(questionMarks, ","),
	)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, nil, fmt.Errorf("Begin: %s", err)
	}

	return tx, stmt, nil
}
