package com.grabds.kafka.connect.exceptions;

public class MissedFieldException extends RuntimeException{

    public MissedFieldException(String message) {
        super(message);
    }
}

