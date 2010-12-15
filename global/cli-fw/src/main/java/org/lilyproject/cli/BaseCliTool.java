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
package org.lilyproject.cli;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.lilyproject.util.exception.StackTracePrinter;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Base framework for Lily CLI tools. Purpose is to have some uniformity in the CLI tools and to avoid
 * code duplication.
 *
 * Subclasses can override:
 * <ul>
 * <li>{@link #getCmdName()}
 * <li>{@link #getOptions()}
 * <li>{@link #processOptions(org.apache.commons.cli.CommandLine)}
 * <li>{@link #run(org.apache.commons.cli.CommandLine)}
 * <li>{@link #reportThrowable(Throwable)}
 * </ul>
 */
public abstract class BaseCliTool {
    protected Option helpOption;
    private Options cliOptions;

    protected void start(String[] args) {
        setupLogging();
        int result = 1;
        try {
            System.out.println();
            result = runBase(args);
        } catch (Throwable t) {
            reportThrowable(t);
        }
        System.out.println();

        if (result != 0)
            System.exit(result);
    }

    protected void reportThrowable(Throwable throwable) {
        StackTracePrinter.printStackTrace(throwable);
    }

    /**
     * Return the CLI options. When overriding, call super and add your own
     * options.
     */
    public List<Option> getOptions() {
        List<Option> options = new ArrayList<Option>();

        helpOption = new Option("h", "help", false, "Shows help");
        options.add(helpOption);

        return options;
    }

    /**
     * The name of this CLI tool, used in the help message.
     */
    protected abstract String getCmdName();

    /**
     * Process option values, typically this performs basic stuff like reading
     * the option value and validating it. First always call super, if non-zero
     * is returned, then return this value immediately.
     */
    protected int processOptions(CommandLine cmd) throws Exception {
        if (cmd.hasOption(helpOption.getOpt())) {
            printHelp();
            return 1;
        }
        return 0;
    }

    /**
     * Perform the actual action. First always call super, if non-zero is returned,
     * then return this value immediately.
     */
    public int run(CommandLine cmd) throws Exception {
        return 0;
    }

    private void setupLogging() {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        Logger log = Logger.getLogger("org.lilyproject.client");
        log.setLevel(Level.INFO);

        final String CONSOLE_LAYOUT = "[%-5p][%d{ABSOLUTE}][%-10.10t] %c - %m%n";

        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setLayout(new PatternLayout(CONSOLE_LAYOUT));

        consoleAppender.activateOptions();
        rootLogger.addAppender(consoleAppender);
    }

    private int runBase(String[] args) throws Exception {
        //
        // Set up options
        //
        cliOptions = new Options();

        for (Option option : getOptions()) {
            cliOptions.addOption(option);
        }

        //
        // Parse options
        //
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.out.println();

            printHelp();
            return 1;
        }

        //
        // Process options
        //
        int result = processOptions(cmd);
        if (result != 0)
            return result;


        //
        // Run tool
        //
        return run(cmd);

    }

    protected void printHelp() throws IOException {
        printHelpHeader();
        HelpFormatter help = new HelpFormatter();
        help.printHelp(getCmdName(), cliOptions, true);
        printHelpFooter();
    }

    protected void printHelpHeader() throws IOException {
        String className = getClass().getName();
        String helpHeaderPath = className.replaceAll(Pattern.quote("."), "/") + "_help_header.txt";
        InputStream is = getClass().getClassLoader().getResourceAsStream(helpHeaderPath);
        if (is != null) {
            IOUtils.copy(is, System.out);
            System.out.println();
            System.out.println();
        }
    }

    protected void printHelpFooter() throws IOException {
        String className = getClass().getName();
        String helpHeaderPath = className.replaceAll(Pattern.quote("."), "/") + "_help_footer.txt";
        InputStream is = getClass().getClassLoader().getResourceAsStream(helpHeaderPath);
        if (is != null) {
            System.out.println();
            IOUtils.copy(is, System.out);
            System.out.println();
        }
    }

}
