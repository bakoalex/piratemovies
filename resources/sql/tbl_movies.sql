CREATE TABLE IF NOT EXISTS movies (
    movie_id SMALLINT NOT NULL AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    year YEAR NOT NULL,
    length SMALLINT NOT NULL,
    media_type ENUM('DVD','VHS') NOT NULL,
    media_cover VARCHAR(255) NOT NULL,
    media_origin ENUM('OFFICIAL', 'PIRATE') NOT NULL,
    num_of_rents SMALLINT NOT NULL,
    is_rented BIT(1) NOT NULL,
    PRIMARY KEY (movie_id)
);