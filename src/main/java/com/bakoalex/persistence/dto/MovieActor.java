package com.bakoalex.persistence.dto;

public class MovieActor {
    private int movieId, actorId;

    public MovieActor() {}
    public MovieActor(int movieId, int actorId) {
        this.movieId = movieId;
        this.actorId = actorId;
    }

    /**
     * @return the actorId
     */
    public int getActorId() {
        return actorId;
    }

    /**
     * @param actorId the actorId to set
     */
    public void setActorId(int actorId) {
        this.actorId = actorId;
    }

    /**
     * @return the movieId
     */
    public int getMovieId() {
        return movieId;
    }

    /**
     * @param movieId the movieId to set
     */
    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }
}