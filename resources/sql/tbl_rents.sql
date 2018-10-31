CREATE TABLE IF NOT EXISTS rents (
    movie_id SMALLINT,
    renter_id SMALLINT,
    rent_start_date DATE NOT NULL,
    rent_end_date DATE,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (renter_id) REFERENCES renter_persons(renter_id)
);