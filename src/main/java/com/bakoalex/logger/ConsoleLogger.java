package com.bakoalex.logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class ConsoleLogger {

    private static Logger logger;
    
    public static final String DB_CONN = "Connecting to database...";
    public static final String DB_CONN_OK = "Connected to database.";
    public static final String DB_CONN_FAIL = "Error when connecting to database: ";
    public static final String DB_QUERY = "Running SQL Query: ";
    public static final String DB_QUERY_EMPTY = "SQL Query response is empty, no record found.";
    public static final String DB_QUERY_DUPL = "The specified entity already exists in the database: ";
    public static final String DB_INSERT_FAIL = "Intertion of the following object to the database failed: ";
    public static final String DB_UPDATE_FAIL = "Update of the following object to the database failed: ";
    public static final String DB_INSERT_OK = "Successfull insert of object: ";
    public static final String DB_UPDATE_OK = "Successfull update of object: ";
    public static final String SQL_EXC = "Exception during SQL operation: ";

    public static class Format extends Formatter {
        public String format(LogRecord rec) {
            StringBuffer buf = new StringBuffer(1000);
            buf.append(calcDate(rec.getMillis()));
            buf.append(rec.getSourceClassName());
            buf.append("::" + rec.getSourceMethodName());
            buf.append(" | " + rec.getLevel() + " | ");
            buf.append(formatMessage(rec));
            buf.append('\n');
            return buf.toString();
        }

        private String calcDate(long millis) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("[yyyy-mm-dd  HH:mm:ss] ");
            Date result = new Date(millis);
            return dateFormat.format(result);
        }
    }

    public static Logger attach() {
        if (logger == null) {
            logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
            logger.setLevel(Level.FINEST);
            logger.setUseParentHandlers(false);

            ConsoleHandler handler = new ConsoleHandler();
            Format format = new ConsoleLogger.Format();
            handler.setFormatter(format);

            logger.addHandler(handler);
        }
        return logger;
    }

}