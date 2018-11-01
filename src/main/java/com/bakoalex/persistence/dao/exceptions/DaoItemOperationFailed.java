package com.bakoalex.persistence.dao.exceptions;

public class DaoItemOperationFailed extends Exception {
    public DaoItemOperationFailed() { super(); }
    public DaoItemOperationFailed(String message) { super(message); }
}