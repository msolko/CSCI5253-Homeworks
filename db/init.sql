CREATE TABLE IF NOT EXISTS dim_animals (
    animal_id VARCHAR(7) PRIMARY KEY,
    name VARCHAR,
    dob DATE,
    animal_type VARCHAR,
    sex VARCHAR, 
    is_fixed BOOL,
    breed VARCHAR,
    color VARCHAR,
    outcome_type VARCHAR,
    outcome_subtype VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id VARCHAR(8) PRIMARY KEY,
    date DATE NOT NULL,
    year INT2  NOT NULL,
    month INT2  NOT NULL,
    day INT2  NOT NULL
);

CREATE TABLE IF NOT EXISTS fct_outcomes (
    outcome_id SERIAL PRIMARY KEY,
    animal_id VARCHAR(7) NOT NULL,
    date_id VARCHAR(8) NOT NULL,
    time TIME NOT NULL,
    FOREIGN KEY (animal_id) REFERENCES dim_animals(animal_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
);