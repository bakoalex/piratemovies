CREATE TABLE IF NOT EXISTS movies_actors (
    movie_id SMALLINT,
    actor_id SMALLINT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
);