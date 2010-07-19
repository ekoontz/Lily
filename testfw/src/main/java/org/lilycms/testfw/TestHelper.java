/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.testfw;

import org.apache.log4j.*;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;

public class TestHelper {
    /**
     * Sets up logging such that errors are logged to the console, and info level
     * logging is sent to a file in target directory.
     *
     * <p>Additionally a set of categories can be specified that will be logged
     * as debug output to the console when a system property -Dlily.test.log is present.
     */
    public static void setupLogging(final String... debugCategories) throws IOException {
        JavaLoggingToLog4jRedirector.activate();

        final String LAYOUT = "[%t] %-5p %c - %m%n";

        Logger logger = Logger.getRootLogger();
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);

        //
        // Log to a file
        //
        FileAppender appender = new FileAppender();
        appender.setLayout(new PatternLayout(LAYOUT));

        // Maven sets a property basedir, but if the testcases are run outside Maven (e.g. by an IDE),
        // then fall back to the working directory
        String targetDir = System.getProperty("basedir");
        if (targetDir == null)
            targetDir = System.getProperty("user.dir");
        String logFileName = targetDir + "/target/log.txt";

        System.out.println("Log output will go to " + logFileName);

        appender.setFile(logFileName, false, false, 0);

        appender.activateOptions();
        logger.addAppender(appender);

        //
        // Add a console appender to show ERROR level errors on the console
        //
        final String CONSOLE_LAYOUT = "[%-5p][%d{ABSOLUTE}][%-10.10t][%30.30c] %m%n";

        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setLayout(new PatternLayout(CONSOLE_LAYOUT));

        final boolean debugLoggingEnabled = System.getProperty("lily.test.log") != null;

        consoleAppender.addFilter(new Filter() {
            @Override
            public int decide(LoggingEvent loggingEvent) {
                if (debugLoggingEnabled) {
                    // This is slow, but it's only for when testcase debug output is enabled
                    for (String debugCat : debugCategories) {
                        if (loggingEvent.getLoggerName().startsWith(debugCat)) {
                            return loggingEvent.getLevel().isGreaterOrEqual(Level.DEBUG) ? Filter.ACCEPT : Filter.DENY;
                        }
                    }
                }

                return loggingEvent.getLevel().isGreaterOrEqual(Level.ERROR) ? Filter.ACCEPT : Filter.DENY;
            }
        });

        consoleAppender.activateOptions();
        logger.addAppender(consoleAppender);


        //
        //
        //
        for (String debugCat : debugCategories) {
            Logger.getLogger(debugCat).setLevel(Level.DEBUG);
        }

    }
}
