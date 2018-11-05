package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"path/filepath"
	"github.com/fatih/color"
)

type Properties struct {
	ConnectString string `yaml:"connectString,omitempty"`
	DefaultSchema string `yaml:"defaultSchema,omitempty"`
}

type Dictionary struct {
	CheckHistoryExists string
	SetDefaultSchema string
	CreateHistory string
	History string
	DefaultSchema string
}

func getProperties(logMode string, headingColor *color.Color) Properties {
	propertiesFileName, _ := filepath.Abs("./properties.yml")
	if logMode == "debug" {
		headingColor.Println("Reading property file :")
		fmt.Println(propertiesFileName)
	}
	propertiesFileContent, err := ioutil.ReadFile(propertiesFileName)
	if logMode == "debug" {
		headingColor.Println("Property file content : \n")
		fmt.Println(string(propertiesFileContent))
	}
	if err != nil {
		log.Fatal(err)
	}
	var properties Properties
	err = yaml.Unmarshal(propertiesFileContent, &properties)
	if err != nil {
		log.Fatal(err)
	}
	if logMode == "debug" {
		propertyString, err := yaml.Marshal(properties)
		if err != nil {
			log.Fatal(err)
		}
		headingColor.Println("Unmarshaled data : ")
		fmt.Println(string(propertyString))
	}
	return properties
}

func setSchema(db *sql.DB, dictionary Dictionary, schema string) error {
	_, err := db.Exec(fmt.Sprintf(dictionary.SetDefaultSchema, schema))
	return err
}

func createHistoryIfNotExisting(logMode string, headingColor *color.Color, db *sql.DB, dictionary Dictionary, schema string, history string) error {
	if logMode == "debug" {
                headingColor.Println("Checking if history exists")
		headingColor.Print("Schema : ")
		fmt.Println(schema)
		headingColor.Print("History : ")
		fmt.Println(history)
        }
	rows, err := db.Query(fmt.Sprintf(dictionary.CheckHistoryExists, schema, history))
	if err != nil {
		log.Fatal(err)
	}
	var exists bool
	for rows.Next() {
		err = rows.Scan(&exists)
		if err != nil {
			log.Fatal(err)
		}
		if exists {
			headingColor.Println("History exists. No action taken.")
		} else {
			headingColor.Println("History does not exist. Creating history.")
			_, err = db.Exec(dictionary.CreateHistory)
			return err
		}
		return err
	}
	return err
}

func main() {
	var dictionary Dictionary
	dictionary.SetDefaultSchema="set search_path='%s'"
	dictionary.CreateHistory="create table databasehistory (fileName varchar(100), statementPos integer, statementHash varchar(100), executionTime timestamp, executionStatus varchar(100), executedBy varchar(100))"
	dictionary.CheckHistoryExists="SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')"
	dictionary.History="databasehistory"
	dictionary.DefaultSchema="public"
	headingColor := color.New(color.FgCyan).Add(color.Bold)
	logMode := "debug"
	properties := getProperties(logMode, headingColor)
	db, err := sql.Open("postgres", properties.ConnectString)
	if err != nil {
		log.Fatal(err)
	}
	if logMode == "debug" {
		headingColor.Println("Executing sql statement")
	}
	err = createHistoryIfNotExisting(logMode, headingColor, db, dictionary, dictionary.DefaultSchema, dictionary.History)
	if err != nil {
		log.Fatal(err)
	}
	err = setSchema(db, dictionary, dictionary.DefaultSchema)
	if err != nil {
		log.Fatal(err)
	}
	age := 21
	rows, err := db.Query("SELECT name FROM users WHERE age = $1", age)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", name)
	}
}
