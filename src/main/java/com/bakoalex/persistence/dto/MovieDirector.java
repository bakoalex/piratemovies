package com.bakoalex.persistence.dto;

public class MovieDirector {
    private int movieId, directorId;

    public MovieDirector() {}
    public MovieDirector(int movieId, int directorId) {
        this.movieId = movieId;
        this.directorId = directorId;
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

    /**
     * @return the directorId
     */
    public int getDirectorId() {
        return directorId;
    }

    /**
     * @param directorId the directorId to set
     */
    public void setDirectorId(int directorId) {
        this.directorId = directorId;
    }
}