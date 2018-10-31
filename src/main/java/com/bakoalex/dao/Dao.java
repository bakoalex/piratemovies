package com.bakoalex.dao;

import java.util.List;

public interface Dao<T> {
    T get(int id);
    List<T> getAll();
    int insert(T t);
    boolean update(T t);
    boolean delete(T t);
}