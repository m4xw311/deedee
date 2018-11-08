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
	"bufio"
	"os"
)

type Properties struct {
	ConnectString string `yaml:"connectString,omitempty"`
	DefaultSchema string `yaml:"defaultSchema,omitempty"`
}

type Dictionary struct {
	CheckHistoryExists string `yaml:"checkHistoryExists,omitempty"`
	SetDefaultSchema string `yaml:"setDefaultSchema,omitempty"`
	CreateHistory string `yaml:"createHistory,omitempty"`
	History string `yaml:"history,omitempty"`
	DefaultSchema string `yaml:"defaultSchema,omitempty"`
	HistoryRecord string `yaml:"historyRecord,omitempty"`
}

func getProperties(logMode string, headingColor *color.Color) Properties {
	propertiesFileName, _ := filepath.Abs("./properties.yml")
	if logMode == "debug" {
		headingColor.Println("Reading property file :")
		fmt.Println(propertiesFileName)
	}
	propertiesFileContent, err := ioutil.ReadFile(propertiesFileName)
	if logMode == "debug" {
		headingColor.Println("Property file content :")
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

func getDictionary(logMode string, headingColor *color.Color) Dictionary {
        dictionaryFileName, _ := filepath.Abs("./dictionary.yml")
        if logMode == "debug" {
                headingColor.Println("Reading dictionary file :")
                fmt.Println(dictionaryFileName)
        }
        dictionaryFileContent, err := ioutil.ReadFile(dictionaryFileName)
        if logMode == "debug" {
                headingColor.Println("Dictionary file content :")
                fmt.Println(string(dictionaryFileContent))
        }
        if err != nil {
                log.Fatal(err)
        }
        var dictionary Dictionary
        err = yaml.Unmarshal(dictionaryFileContent, &dictionary)
        if err != nil {
                log.Fatal(err)
        }
        if logMode == "debug" {
                dictionaryString, err := yaml.Marshal(dictionary)
                if err != nil {
                        log.Fatal(err)
                }
                headingColor.Println("Unmarshaled data : ")
                fmt.Println(string(dictionaryString))
        }
        return dictionary
}

func setSchema(logMode string, headingColor *color.Color, db *sql.DB, dictionary Dictionary, schema string) error {
	if logMode == "debug" {
		headingColor.Print("Schema : ")
                fmt.Println(schema)
		headingColor.Print("Set Default Schmea: ")
                fmt.Println(dictionary.SetDefaultSchema)
	}
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
func executeStatement(logMode string, headingColor *color.Color, db *sql.DB, dictionary Dictionary, schema string, history string, statement string) (*sql.Rows, error) {
	if logMode == "debug" {
		headingColor.Print("Executing sql statement: ")
		fmt.Println(statement)
	}
	rows, err := db.Query(statement)
	return rows, err
}
func main() {
	headingColor := color.New(color.FgCyan).Add(color.Bold)
	logMode := "debug"
	properties := getProperties(logMode, headingColor)
	dictionary := getDictionary(logMode, headingColor)
	db, err := sql.Open("postgres", properties.ConnectString)
	if err != nil {
		log.Fatal(err)
	}
	err = createHistoryIfNotExisting(logMode, headingColor, db, dictionary, dictionary.DefaultSchema, dictionary.History)
	if err != nil {
		log.Fatal(err)
	}
	err = setSchema(logMode, headingColor, db, dictionary, dictionary.DefaultSchema)
	if err != nil {
		fmt.Println("h");
		log.Fatal(err)
	}
	fileHandle, _ := os.Open("main.sql")
	defer fileHandle.Close()
	fileScanner := bufio.NewScanner(fileHandle)
	statement:=""
	inverse:=""
	for fileScanner.Scan() {
		line := fileScanner.Text()
		if (line[0:2]!="--") {
			statement=statement+line+"\n"
			inverse=""
		} else {
			fmt.Println(statement)
			statement=""
			inverse=inverse+line+"\n"
		}
	}
	fmt.Println(statement)
	rows, err := executeStatement(logMode, headingColor, db, dictionary, dictionary.DefaultSchema, dictionary.History, "UPDATE users set name=name||'z' where age=21; SELECT name,age FROM users WHERE age = 21; UPDATE users set name=name||'1' where age=21;commit;SELECT age FROM users WHERE age = 21;")
	if err != nil {
		log.Fatal(err)
	}
	cols, err := rows.Columns()
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			log.Fatal(err)
		}
		for _, val := range vals {
			fmt.Printf("%s\n", string(*val.(*sql.RawBytes)))
		}
	}
}
