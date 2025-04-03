CREATE TABLE envejecimiento (
    id_pais INT,
    nombre_pais VARCHAR(100),
    capital VARCHAR(100),
    continente VARCHAR(50),
    region VARCHAR(100),
    poblacion DECIMAL(15,2), 
    tasa_de_envejecimiento DECIMAL(5,2)
);

CREATE TABLE poblacion_costos (
    id VARCHAR(36) PRIMARY KEY, 
    continente VARCHAR(50),
    pais VARCHAR(100),
    poblacion BIGINT,
    costo_bajo_hospedaje DECIMAL(10,2),
    costo_promedio_comida DECIMAL(10,2),
    costo_bajo_transporte DECIMAL(10,2),
    costo_promedio_entretenimiento DECIMAL(10,2)
);


-- Cargar datos en poblacion_costos
COPY poblacion_costos (id, continente, pais, poblacion, costo_bajo_hospedaje, costo_promedio_comida, costo_bajo_transporte, costo_promedio_entretenimiento) 
FROM 'C:/Program Files/PostgreSQL/17/bin/pais_poblacion.csv'
DELIMITER ','
CSV HEADER;

-- Cargar datos en envejecimiento
COPY envejecimiento(id_pais, nombre_pais, capital, continente, region, poblacion, tasa_de_envejecimiento) 
FROM 'C:/Program Files/PostgreSQL/17/bin/pais_envejecimiento.csv'
DELIMITER ','
CSV HEADER;

ALTER TABLE envejecimiento
ALTER COLUMN poblacion TYPE NUMERIC;

ALTER TABLE poblacion_costos
ALTER COLUMN id TYPE VARCHAR(50);


