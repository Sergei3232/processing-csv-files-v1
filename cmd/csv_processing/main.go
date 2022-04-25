package main

import (
	"encoding/csv"
	"fmt"
	sq_postgres "github.com/Sergei3232/processing-csv-files-v1/internal/postgres"
	"github.com/joho/godotenv"
	"io/ioutil"
	"log"
	"os"
)

type fileData struct {
	sku             int64
	mapi_item       int64
	vertica_variant int64
	height          int64
	width           int64
}

func main() {
	fmt.Println("---------------------Start script---------------------")
	_ = godotenv.Load()

	fileName, found := os.LookupEnv("FILE_TEST_CSV")
	if !found {
		log.Panic("environment variable FILE_TEST_CSV not found in .env")
	}
	fmt.Println(fileName)

	dataSourceNameCSV, found := os.LookupEnv("SQLCONNECT_CSV")
	if !found {
		log.Panic("environment variable SQLCONNECT not found in .env")
	}

	dataSourceNameFileLoader, found := os.LookupEnv("SQLCONNECT_FILE_LOADER")
	if !found {
		log.Panic("environment variable SQLCONNECT not found in .env")
	}

	dataSourceNameFileStorage, found := os.LookupEnv("SQLCONNECT_FILE_STORAGE")
	if !found {
		log.Panic("environment variable SQLCONNECT not found in .env")
	}

	dbFileStorage, sqErrorCSV := sq_postgres.NewDBConnect(dataSourceNameFileStorage)
	if sqErrorCSV != nil {
		log.Println(sqErrorCSV)
	}
	defer dbFileStorage.Close()

	dbCSV, sqErrorCSV := sq_postgres.NewDBConnect(dataSourceNameCSV)
	if sqErrorCSV != nil {
		log.Println(sqErrorCSV)
	}
	defer dbCSV.Close()

	dbImageLoader, sqError := sq_postgres.NewDBConnect(dataSourceNameFileLoader) //переименовать
	if sqError != nil {
		log.Println(sqError)
	}
	defer dbImageLoader.Close()

	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	log.Println(dbCSV, path, dbImageLoader)
	n := 0
	//обработка получения высоты и ширины главных изображений

	for dbCSV.FurtherProcessingIsNeeded() {
		listData, err := dbCSV.GetListRecordsProcess(1000)
		if err != nil {
			log.Println(err)
		}

		dbImageLoader.GettingIdImageFileStorage(listData)
		dbFileStorage.GetImageHightWidth(listData)
		dbCSV.Update(listData)
		n += 1000
		fmt.Println(n)
	}

	//Выгрузка CSV
	//csvWriterTest(dbCSV, fileName)

	//Чтение файла нужно будет нормально реализовать
	//filaP := path+"/mapi_item_7500.csv"
	//if err := parseLocationCSV(filaP, dbCSV, "mapi_item_7500.csv"); err != nil {
	//	fmt.Println(err)
	//}
	fmt.Println("---------------------End script---------------------")
}

func parseLocationCSV(file string, db_csv *sq_postgres.PostgresSQLDB, fileName string) error {
	f, err := os.Open(file)
	defer f.Close()

	if err != nil {
		return err
	}
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return err
	}

	for nl, line := range lines {
		if nl > 0 {
			db_csv.InsertDataCSVFile(line, fileName)
			fmt.Println(line)
		}
		nl++
	}
	return nil
}

func csvWriterTest(dbCSV *sq_postgres.PostgresSQLDB, fileName string) {
	records := [][]string{
		{"sku", "mapi_item", "vertica_variant", "height", "width"},
	}
	dataToAdd, errdbCSV := dbCSV.GetUploadData()
	if errdbCSV != nil {
		log.Panic(errdbCSV)
	}
	records = append(records, dataToAdd...)

	file, errCreate := os.Create(fileName)
	if errCreate != nil {
		log.Panic(errCreate)
	}

	w := csv.NewWriter(file)

	for _, record := range records {
		if err := w.Write(record); err != nil {
			log.Fatalln("error writing record to csv:", err)
		}
	}

	// Записываем любые буферизованные данные в подлежащий writer (стандартный вывод).
	w.Flush()

	err := ioutil.WriteFile("myfile.csv", []byte{}, 0777)
	// Обработка ошибки
	if err != nil {
		// print it out
		fmt.Println(err)
	}

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}
