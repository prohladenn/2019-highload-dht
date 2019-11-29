package ru.mail.polis.prohladenn;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Utils {

    public static ByteBuffer strToBB(String string) {
        return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
    }
}
