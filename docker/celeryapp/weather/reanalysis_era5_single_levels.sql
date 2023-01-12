CREATE TABLE "Municipio".copernicus_br (
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

CREATE INDEX CONCURRENTLY IF NOT EXISTS date_idx ON "Municipio".copernicus_br (date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS geocodigo_idx ON "Municipio".copernicus_br (geocodigo);

-- Foz do Iguaçu
CREATE TABLE "Municipio".copernicus_foz (
    date TIMESTAMP WITHOUT TIME ZONE,
    geocodigo BIGINT,
    temp FLOAT(23),
    precip FLOAT(23),
    pressao FLOAT(23),
    umid FLOAT(23)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS date_idx ON "Municipio".copernicus_foz (date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS geocodigo_idx ON "Municipio".copernicus_foz (geocodigo);
