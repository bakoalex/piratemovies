package com.bakoalex.persistence.dao.exceptions;

public class DaoItemDuplicate extends Exception {
    public DaoItemDuplicate() { super(); } 
    public DaoItemDuplicate(String message) { super(message); }
}