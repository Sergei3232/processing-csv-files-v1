package postgres

import (
	"database/sql"
	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
	"log"
	"strconv"
)

type DataSCV struct {
	Id            int64
	IdFileStorage int64
	Height        int64
	Width         int64
}

type PostgresSQLDB struct {
	*sql.DB
	qb sq.StatementBuilderType
}

func NewDBConnect(sqlConnect string) (*PostgresSQLDB, error) {
	bd, err := sql.Open("postgres", sqlConnect) //postgres
	if err != nil {
		return &PostgresSQLDB{}, err
	}

	return &PostgresSQLDB{bd, sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}, nil
}

func (p *PostgresSQLDB) InsertDataCSVFile(records []string, fileName string) {

	sqlStatement := `
		INSERT INTO files_csv (file_name, sku, mapi_item, vertica_variant)
		VALUES ($1, $2, $3, $4)`

	a1, _ := strconv.Atoi(records[0])
	a2, _ := strconv.Atoi(records[1])
	a3, _ := strconv.Atoi(records[2])

	_, err := p.DB.Exec(sqlStatement, fileName, a1, a2, a3)
	if err != nil {
		log.Fatalln(err)
	}

}

func (p *PostgresSQLDB) GetListRecordsProcess(portion uint64) (map[int64]DataSCV, error) {
	mapIdItems := make(map[int64]DataSCV)
	sqlStatement, args, err := sq.Select("mapi_item, id").
		From("files_csv").
		Where("id_file_storage is null").Limit(portion).ToSql()
	if err != nil {
		return nil, err
	}

	rows, errDB := p.DB.Query(sqlStatement, args...)
	defer rows.Close()
	if errDB != nil {
		return nil, errDB
	}

	for rows.Next() {
		var idItem, id int64
		if err := rows.Scan(&idItem, &id); err != nil {
			return nil, err
		}
		mapIdItems[idItem] = DataSCV{id, 0, 0, 0}
	}
	return mapIdItems, nil
}

func (p *PostgresSQLDB) FurtherProcessingIsNeeded() bool {
	sqlStatement, args, err := sq.Select("mapi_item, id").
		From("files_csv").
		Where("id_file_storage is null").Limit(1).ToSql()
	if err != nil {
		return false
	}

	rows, errDB := p.DB.Query(sqlStatement, args...)
	defer rows.Close()
	if errDB != nil {
		return false
	}
	for rows.Next() {
		return true
	}
	return false
}

func (p *PostgresSQLDB) Update(mapIdItems map[int64]DataSCV) error {
	for _, val := range mapIdItems {
		sqlStatement, args, err := p.qb.Update("files_csv").
			Set("id_file_storage", val.IdFileStorage).
			Set("height", val.Height).
			Set("width", val.Width).
			Where(sq.Eq{"id": val.Id}).
			ToSql()

		if err != nil {
			log.Println(err)
		}

		_, errDB := p.DB.Exec(sqlStatement, args...)

		if errDB != nil {
			log.Println(errDB)
		}
	}
	return nil
}

func (p *PostgresSQLDB) GetUploadData() ([][]string, error) {
	arrayDataDB := make([][]string, 0, 500000)

	sqlStatement, args, err := sq.Select("sku, mapi_item, vertica_variant, height, width").
		From("files_csv").
		Where("id_file_storage is not null").
		OrderBy("id").ToSql()

	if err != nil {
		return [][]string{}, err
	}

	rows, errDB := p.DB.Query(sqlStatement, args...)
	if errDB != nil {
		return nil, err
	}

	for rows.Next() {
		var sku, mapiItem, verticaVariant, height, width int64
		if err := rows.Scan(&sku, &mapiItem, &verticaVariant, &height, &width); err != nil {
			return nil, err
		}
		arrayDataDB = append(arrayDataDB, []string{strconv.Itoa(int(sku)), strconv.Itoa(int(mapiItem)), strconv.Itoa(int(verticaVariant)), strconv.Itoa(int(height)), strconv.Itoa(int(width))})
	}
	return arrayDataDB, nil
}
