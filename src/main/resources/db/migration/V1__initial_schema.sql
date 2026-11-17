CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY,
    status INT NOT NULL,
    created TIMESTAMP NOT NULL,
    data_table CHAR(37)
);

CREATE TABLE IF NOT EXISTS aliases (
    id SERIAL PRIMARY KEY,
    dataset_id UUID NOT NULL,
    alias TEXT NOT NULL
);
