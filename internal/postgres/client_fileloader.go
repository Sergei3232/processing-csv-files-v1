package postgres

import (
	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"strings"
)

func (p *PostgresSQLDB) GettingIdImageFileStorage(mapIdItems map[int64]DataSCV) {

	imageIDs := make([]int64, 0, 100)
	for key := range mapIdItems {
		imageIDs = append(imageIDs, key)
	}

	sqlStatement, args, err := p.qb.Select("ii.item_id, i.url").
		From("item_images AS ii").
		Join("image AS i on i.id = ii.image_ids[1]").
		Where(sq.Eq{"ii.item_id": imageIDs}).
		ToSql()

	if err != nil {
		log.Println(err)
	}

	rows, errDB := p.DB.Query(sqlStatement, args...)
	if errDB != nil {
		log.Println(errDB)
	}

	for rows.Next() {
		var idItem int64
		var url string
		if err := rows.Scan(&idItem, &url); err != nil {
			log.Println(err)
		}
		if url != "" {
			idImage, err := parsUrl(url)
			if err != nil {
				log.Println(err)
			}
			if val, ok := mapIdItems[idItem]; ok {
				val.IdFileStorage = idImage
				mapIdItems[idItem] = val
			}

		}
	}
}

func parsUrl(url string) (int64, error) {
	stringPars := strings.Split(url, "/")
	stringsImage := strings.Split(stringPars[len(stringPars)-1], ".")
	idImage, err := strconv.ParseInt(stringsImage[0], 10, 0)
	if err != nil {
		return 0, err
	}
	return idImage, nil
}
