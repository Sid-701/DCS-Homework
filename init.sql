CREATE TABLE animal_data (
    animal_id VARCHAR(255) PRIMARY KEY,
    breed_id INT,
    name VARCHAR(255),
    date_of_birth DATE,
    animal_type VARCHAR(255),
    FOREIGN KEY (breed_id) REFERENCES breed_type(breed_id)
);

CREATE TABLE breed_type (
    breed_id INT PRIMARY KEY,
    breed VARCHAR(255)
);

CREATE TABLE outcome_events (
    outcome_event_id INT PRIMARY KEY,
    datetime TIMESTAMP,
    animal_id VARCHAR(255),
    outcome_type_id INT,
    FOREIGN KEY (animal_id) REFERENCES animal(animal_id),
    FOREIGN KEY (outcome_type_id) REFERENCES outcome_type(outcome_type_id)
);

CREATE TABLE fact_table (
    animal_id VARCHAR(255) NOT NULL,
    breed_id INT,
    outcome_event_id INT,
    FOREIGN KEY (animal_id) REFERENCES animal_data(animal_id),
    FOREIGN KEY(breed_id) REFERENCES breed_type(breed_id),
    FOREIGN KEY (outcome_event_id) REFERENCES outcome_events(outcome_event_id)
    
);
