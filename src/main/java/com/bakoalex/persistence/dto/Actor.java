package com.bakoalex.persistence.dto;

import java.util.Objects;

public class Actor {
    private int actorId;
    private String name;

    public Actor() {}
    public Actor(String name) {
        this.name = name;
    }
    public Actor(int actorId, String name) {
        this.actorId = actorId;
        this.name = name;
    }

    public int getActorId() {
        return actorId;
    }

    public void setActorId(int actorId) {
        this.actorId = actorId;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Actor(actor_id=" + this.actorId + ", name=" + this.name + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;
        if (!(o instanceof Actor)) return false;
        Actor actor = (Actor) o;
        return actorId == actor.actorId && name.equals(actor.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actorId, name);
    }

}