package com.bakoalex.dto;

import java.util.List;
import java.util.Objects;

public class Movie {
    private int movieId;
    private String title;
    private List<Director> directors;
    private List<Actor> actors;
    private int year;
    private int length;
    private String mediaType;
    private String mediaCover;
    private String mediaOrigin;
    private int numOfRents;
    private boolean isRented;
    

    public Movie() {}
    public Movie(String title, List<Director> directors, List<Actor> actors, int year, int length, String mediaType, String mediaCover, String mediaOrigin, int numOfRents, boolean isRented) {
        this.title = title;
        this.directors = directors;
        this.actors = actors;
        this.year = year;
        this.length = length;
        this.mediaType = mediaType;
        this.mediaCover = mediaCover;
        this.mediaOrigin = mediaOrigin;
        this.numOfRents = numOfRents;
        this.isRented = isRented;
    }
    public Movie(int movieId, String title, List<Director> directors, List<Actor> actors, int year, int length, String mediaType, String mediaCover, String mediaOrigin, int numOfRents, boolean isRented) {
        this.movieId = movieId;
        this.title = title;
        this.directors = directors;
        this.actors = actors;
        this.year = year;
        this.length = length;
        this.mediaType = mediaType;
        this.mediaCover = mediaCover;
        this.mediaOrigin = mediaOrigin;
        this.numOfRents = numOfRents;
        this.isRented = isRented;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    /**
     * @return the directors
     */
    public List<Director> getDirectors() {
        return directors;
    }

    /**
     * @param directors the directors to set
     */
    public void setDirectors(List<Director> directors) {
        this.directors = directors;
    }

    /**
     * @return the actors
     */
    public List<Actor> getActors() {
        return actors;
    }

    /**
     * @param actors the actors to set
     */
    public void setActors(List<Actor> actors) {
        this.actors = actors;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int lenght) {
        this.length = lenght;
    }

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    public String getMediaCover() {
        return mediaCover;
    }

    public void setMediaCover(String mediaCover) {
        this.mediaCover = mediaCover;
    }

    public String getMediaOrigin() {
        return mediaOrigin;
    }

    public void setMediaOrigin(String mediaOrigin) {
        this.mediaOrigin = mediaOrigin;
    }

    public int getNumOfRents() {
        return numOfRents;
    }

    public void setNumOfRents(int numOfRents) {
        this.numOfRents = numOfRents;
    }

    public boolean isRented() {
        return isRented;
    }

    public void setRented(boolean isRented) {
        this.isRented = isRented;
    }

    @Override
    public String toString() {
        String actors = "Actors: ";
        for (Actor a : this.actors) actors += a.getName() + ", ";
        String directors = "Directors: ";
        for (Director d : this.directors) directors += d.getName() + ", ";
        return this.movieId + " " + this.title + " " + directors + " " + actors + " " + this.year;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o == this) return true;
        if (!(o instanceof Movie)) return false;
        Movie movie = (Movie) o;
        return  movieId == movie.movieId &&
                title.equals(movie.title) &&
                actors.equals(movie.actors) &&
                directors.equals(movie.directors) &&
                year == movie.year &&
                length == movie.length &&
                getMediaType().equals(movie.getMediaType()) &&
                mediaCover.equals(movie.mediaCover) &&
                getMediaOrigin().equals(movie.getMediaOrigin()) &&
                numOfRents == movie.numOfRents &&
                isRented == movie.isRented;
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, title, year, length, getMediaType(), mediaCover, getMediaOrigin(), numOfRents, isRented);
    }

}