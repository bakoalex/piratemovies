package com.bakoalex.persistence.dto;

import java.util.Objects;

public class Director implements Comparable<Director> {
    private int directorId;
    private String name;

    public Director() {}
    public Director(String name) {
        this.name = name;
    }
    public Director(int directorId, String name) {
        this.directorId = directorId;
        this.name = name;
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

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(Director otheDirector) {
        return this.name.compareTo(otheDirector.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directorId, name);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Director)) return false;
        Director other = (Director) o;
        return directorId == other.directorId && name.equals(other.name);
    }
}