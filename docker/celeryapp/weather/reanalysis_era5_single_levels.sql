CREATE TABLE IF NOT EXISTS weather.copernicus_brasil (
    index SERIAL UNIQUE PRIMARY KEY,
    date TIMESTAMP WITHOUT TIME ZONE,
    geocodigo BIGINT,
    temp_min FLOAT(23),
    temp_med FLOAT(23),
    temp_max FLOAT(23),
    precip_min FLOAT(23),
    precip_med FLOAT(23),
    precip_max FLOAT(23),
    pressao_min FLOAT(23),
    pressao_med FLOAT(23),
    pressao_max FLOAT(23),
    umid_min FLOAT(23),
    umid_med FLOAT(23),
    umid_max FLOAT(23)
);

-- Foz do Iguaçu
CREATE TABLE IF NOT EXISTS weather.copernicus_foz_do_iguacu (
    index SERIAL UNIQUE PRIMARY KEY,
    date DATETIME NOT NULL,
    geocodigo BIGINT NOT NULL,
    temp FLOAT(23) NOT NULL,
    precip FLOAT(23) NOT NULL,
    pressao FLOAT(23) NOT NULL,
    umid FLOAT(23) NOT NULL,
);
