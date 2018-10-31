package com.bakoalex;

import java.util.ArrayList;
import java.util.List;

import com.bakoalex.dao.DirectorDao;
import com.bakoalex.dao.MovieDao;
import com.bakoalex.dto.Director;
import com.bakoalex.dto.Movie;

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
        System.out.println(movieDao.get(1).toString());

        System.out.println("Exiting application.");
    }
}
