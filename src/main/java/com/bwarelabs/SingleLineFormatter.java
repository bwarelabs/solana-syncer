package com.bwarelabs;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class SingleLineFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
        return String.format("%1$tF %1$tT %2$-7s [%3$s] %4$s - %5$s%n",
                record.getMillis(),
                record.getLevel().getName(),
                record.getThreadID(),
                record.getLoggerName(),
                formatMessage(record));
    }
}
