package postgres

import (
	sq "github.com/Masterminds/squirrel"
	"log"
)

func (p *PostgresSQLDB) GetImageHightWidth(mapIdItems map[int64]DataSCV) error {

	for key, val := range mapIdItems {
		sqlStatement, args, err := p.qb.Select("height,width").
			From("image").
			Where(sq.Eq{"id": val.IdFileStorage}).
			ToSql()
		if err != nil {
			log.Println(err)
			return err
		}

		rows, errDB := p.DB.Query(sqlStatement, args...)
		if errDB != nil {
			log.Println(errDB)
		}

		for rows.Next() {
			var height, width int64
			if err := rows.Scan(&height, &width); err != nil {
				log.Println(err)
			}
			val.Width, val.Height = width, height
			mapIdItems[key] = val
		}
	}
	return nil
}
