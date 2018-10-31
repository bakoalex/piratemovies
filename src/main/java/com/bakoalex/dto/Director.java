package com.bakoalex.dto;

public class Director {
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
}