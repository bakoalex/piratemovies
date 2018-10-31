CREATE TABLE IF NOT EXISTS movies_directors (
    movie_id SMALLINT,
    director_id SMALLINT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (director_id) REFERENCES directors(director_id)
);