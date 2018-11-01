package com.bakoalex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.bakoalex.persistence.dao.impl.MovieDao;
import com.bakoalex.persistence.dto.Actor;
import com.bakoalex.persistence.dto.Movie;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println("Starting Application.");

        MovieDao movieDao = new MovieDao();
        Movie movie = movieDao.get(1);
        System.out.println(movie.toString());

        List<Actor> actors = new ArrayList<>();
        actors.add(new Actor("Keanu Reeves"));

        movie.setTitle("John Wick");
        movie.setActors(actors);
        movie.setMovieId(10);

        System.out.println(movie.toString());

        movieDao.insert(movie);


        System.out.println("Exiting application.");
    }
}
