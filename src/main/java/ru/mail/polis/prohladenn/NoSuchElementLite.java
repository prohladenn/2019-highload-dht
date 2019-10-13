package ru.mail.polis.prohladenn;

import java.util.NoSuchElementException;

public class NoSuchElementLite extends NoSuchElementException {
    public NoSuchElementLite(String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
