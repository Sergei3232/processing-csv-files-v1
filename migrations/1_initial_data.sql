-- +goose Up
-- +goose StatementBegin
create table files_csv
(
    id SERIAL primary key,
    file_name varchar not null,
    sku integer ,
    mapi_item integer,
    vertica_variant bigint,
    id_file_storage bigint,
    height integer,
    width integer
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop table if exists files_csv;
-- +goose StatementEnd