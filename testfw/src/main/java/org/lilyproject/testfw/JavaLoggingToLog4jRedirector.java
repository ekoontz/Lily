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
package org.lilyproject.testfw;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Redirects Java logging to log4j.
 *
 * <p>This code comes from http://wiki.apache.org/myfaces/Trinidad_and_Common_Logging
 * but slightly adapted to log4j instead of commons-logging.
 *
 * <p>It would actually be better to redirect to commons-logging, but the SOLR war
 * which is launched as part of certain test cases contains the jcl-slf4j bridge and
 * uses jdk14-slf4j, which would cause endless loops between logging systems.
 */
public class JavaLoggingToLog4jRedirector {
    static JDKLogHandler activeHandler;

    /**
     * Activates this feature.
     */
    public static void activate() {
        try {
            Logger rootLogger = LogManager.getLogManager().getLogger("");
            // remove old handlers
            for (Handler handler : rootLogger.getHandlers()) {
                rootLogger.removeHandler(handler);
            }
            // add our own
            activeHandler = new JDKLogHandler();
            activeHandler.setLevel(Level.ALL);
            rootLogger.addHandler(activeHandler);
            rootLogger.setLevel(Level.ALL);
            // done, let's check it right away!!!

            Logger.getLogger(JavaLoggingToLog4jRedirector.class.getName()).info("activated: sending JDK log messages to Commons Logging");
        } catch (Exception exc) {
            org.apache.log4j.Logger.getLogger(JavaLoggingToLog4jRedirector.class).error("activation failed", exc);
        }
    }

    public static void deactivate() {
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.removeHandler(activeHandler);

        Logger.getLogger(JavaLoggingToLog4jRedirector.class.getName()).info("dactivated");
    }

    protected static class JDKLogHandler extends Handler {
        private Map<String, org.apache.log4j.Logger> cachedLogs = new ConcurrentHashMap<String, org.apache.log4j.Logger>();

        private org.apache.log4j.Logger getLog(String logName) {
            org.apache.log4j.Logger log = cachedLogs.get(logName);
            if (log == null) {
                log = org.apache.log4j.Logger.getLogger(logName);
                cachedLogs.put(logName, log);
            }
            return log;
        }

        @Override
        public void publish(LogRecord record) {
            org.apache.log4j.Logger logger = getLog(record.getLoggerName());
            String message = record.getMessage();
            Throwable exception = record.getThrown();
            Level level = record.getLevel();
            if (level == Level.SEVERE) {
                logger.error(message, exception);
            } else if (level == Level.WARNING) {
                logger.warn(message, exception);
            } else if (level == Level.INFO) {
                logger.info(message, exception);
            } else if (level == Level.CONFIG) {
                logger.debug(message, exception);
            } else {
                logger.trace(message, exception);
            }
        }

        @Override
        public void flush() {
            // nothing to do
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}