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
package org.lilyproject.tools.tester;

import de.svenjacobs.loremipsum.LoremIpsum;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.metrics.*;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.repository.api.*;
import org.lilyproject.tools.import_.cli.*;
import org.lilyproject.tools.import_.json.*;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;
import org.lilyproject.util.exception.StackTracePrinter;
import org.lilyproject.util.io.Closer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Tester extends BaseCliTool {
    private LilyClient client;
    private Repository repository;

    private int createCount;
    private int readCount;
    private int updateCount;
    private int deleteCount;

    private int maximumRunTime;
    private int maximumRecordCreated;
    private int maximumFailures;

    private String zookeeperConnectString;
    private String reportFileName;
    private String failuresFileName;

    private long startTime;
    private int failureCount;

    private List<Field> fields;
    private LoremIpsum loremIpsum = new LoremIpsum();
    private RecordType recordType;
    private List<TestRecord> records = new ArrayList<TestRecord>(50000);

    private PrintStream reportStream;
    private PrintStream errorStream;

    private String metricsRecordName;
    private TesterMetrics metrics;

    private Option configFileOption;
    private Option dumpSampleConfigOption;
    private Option logDirOption;

    private File logDirPath = null;
    private int iteration = 0;
    private DurationMetrics durationMetrics; 
    
    @Override
    protected String getCmdName() {
        return "lily-tester";
    }

    public static void main(String[] args) throws Exception {
        new Tester().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        configFileOption = OptionBuilder
                .withArgName("config.json")
                .hasArg()
                .withDescription("Test tool configuration file")
                .withLongOpt("config")
                .create("c");
        options.add(configFileOption);

        dumpSampleConfigOption = OptionBuilder
                .withDescription("Dumps a sample configuration to standard out.")
                .withLongOpt("dump-sample-config")
                .create("d");
        options.add(dumpSampleConfigOption);
        
        logDirOption = OptionBuilder
            .withArgName("logDir")
            .hasArg()
            .withDescription("Directory to put the logfiles into.")
            .withLongOpt("logdir")
            .create("l");
        options.add(logDirOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        if (cmd.hasOption(dumpSampleConfigOption.getOpt())) {
            InputStream is = getClass().getClassLoader().getResourceAsStream("org/lilyproject/tools/tester/config.json");
            try {
                IOUtils.copy(is, System.out);
            } finally {
                Closer.close(is);
            }
            return 0;
        }

        if (!cmd.hasOption(configFileOption.getOpt())) {
            printHelp();
            return 1;
        }
        
        if (cmd.hasOption(logDirOption.getOpt())) {
            String logDir = cmd.getOptionValue(logDirOption.getOpt());
            System.out.println("LogDir = " + logDir);
            logDirPath = new File(logDir);
        }

        String configFileName = cmd.getOptionValue(configFileOption.getOpt());
        InputStream is = new FileInputStream(configFileName);
        JsonNode configNode = JsonFormat.deserializeNonStd(is);
        is.close();

        readConfig(configNode);

        client = new LilyClient(zookeeperConnectString, 10000);
        repository = client.getRepository();

        createSchema(configNode);

        if (metricsRecordName != null) {
            System.out.println("Enabling metrics.");
            metrics = new TesterMetrics(metricsRecordName);
        }

        openStreams();
        try {            
            System.out.println("Running tests...");
            System.out.println("Tail the output files if you wonder what is happening.");

            test();
        } finally {
            closeStreams();
        }

        System.out.println("Total records created during test: " + records.size());
        System.out.println("Total failures: " + failureCount);

        Closer.close(client);

        return 0;
    }

    private void openStreams() throws FileNotFoundException {
        durationMetrics = new DurationMetrics(new File(logDirPath, "metrics.txt"));
        reportStream = new PrintStream(new File(logDirPath, reportFileName));
        reportStream.println("Date,Iteration,Success/Failure,Create/Read/Update/Delete,Duration (ms)");
        errorStream = new PrintStream(new File(logDirPath, failuresFileName));
        reportStream.print(new DateTime() + " Opening file");
        errorStream.print(new DateTime() + " Opening file");
    }

    private void closeStreams() {
        durationMetrics.finish();
        reportStream.print(new DateTime() + " Closing file");
        errorStream.print(new DateTime() + " Closing file");
        Closer.close(reportStream);
        Closer.close(errorStream);
    }

    public void readConfig(JsonNode configNode) throws IOException {
        JsonNode scenario = JsonUtil.getNode(configNode, "scenario");
        createCount = JsonUtil.getInt(scenario, "creates");
        readCount = JsonUtil.getInt(scenario, "reads");
        updateCount = JsonUtil.getInt(scenario, "updates");
        deleteCount = JsonUtil.getInt(scenario, "deletes");

        if (createCount < 1) {
            throw new RuntimeException("Number of creates should be at least 1.");
        }

        if (deleteCount >= createCount) {
            throw new RuntimeException("Number of deletes should be less than number of creates.");
        }

        JsonNode stopConditions = JsonUtil.getNode(configNode, "stopConditions");
        maximumRunTime = JsonUtil.getInt(stopConditions, "maximumRunTime");
        maximumRecordCreated = JsonUtil.getInt(stopConditions, "maximumRecordsCreated");
        maximumFailures = JsonUtil.getInt(stopConditions, "maximumFailures");

        zookeeperConnectString = JsonUtil.getString(configNode, "zookeeper");
        reportFileName = JsonUtil.getString(configNode, "reportFile");
        failuresFileName = JsonUtil.getString(configNode, "failuresFile");

        JsonNode metricsNode = configNode.get("metrics");
        if (metricsNode != null) {
            metricsRecordName = JsonUtil.getString(metricsNode, "recordName", null);

            ContextFactory contextFactory = ContextFactory.getFactory();

            String className = JsonUtil.getString(metricsNode, "class", null);
            if (className != null)
                contextFactory.setAttribute("tester.class", className);

            String period = JsonUtil.getString(metricsNode, "period", null);
            if (period != null)
                contextFactory.setAttribute("tester.period", period);

            String servers = JsonUtil.getString(metricsNode, "servers", null);
            if (servers != null)
                contextFactory.setAttribute("tester.servers", servers);
        }
    }

    public void createSchema(JsonNode configNode) throws IOException, RepositoryException, ImportConflictException,
            ImportException, JsonFormatException, NoServersException, InterruptedException, KeeperException {

        JsonImport jsonImport = new JsonImport(repository, new DefaultImportListener());

        // Namespaces
        ObjectNode namespacesNode = JsonUtil.getObject(configNode, "namespaces", null);
        if (namespacesNode != null) {
            jsonImport.readNamespaces(namespacesNode);
        }

        // Fields
        fields = new ArrayList<Field>();
        JsonNode fieldTypes = configNode.get("fieldTypes");
        if (fieldTypes != null && fieldTypes.isArray()) {
            for (JsonNode node : fieldTypes) {
                FieldType importFieldType = jsonImport.importFieldType(node);
                fields.add(new Field(importFieldType));
            }
        }

        // Record type
        String recordTypeName = JsonUtil.getString(configNode, "recordTypeName");
        QName recordTypeQName = QNameConverter.fromJson(recordTypeName, jsonImport.getNamespaces());
        recordType = repository.getTypeManager().newRecordType(recordTypeQName);
        for (Field field : fields) {
            recordType.addFieldTypeEntry(field.fieldType.getId(), false);
        }

        recordType = jsonImport.importRecordType(recordType);
    }

    private void test() {
        startTime = System.currentTimeMillis();
        while (true) {
            iteration++;
            testCreate();
            if (checkStopConditions()) return;

            testRead();
            if (checkStopConditions()) return;

            testUpdate();
            if (checkStopConditions()) return;

            testDelete();
            if (checkStopConditions()) return;
        }
    }

    private void testDelete() {
        for (int i = 0; i < deleteCount; i++) {
            TestRecord testRecord = getNonDeletedRecord();

            if (testRecord == null)
                continue;

            long before = System.currentTimeMillis();
            try {
                repository.delete(testRecord.record.getId());
                long after = System.currentTimeMillis();
                testRecord.deleted = true;
                report(Action.DELETE, true, (int)(after - before));
            } catch (Throwable t) {
                long after = System.currentTimeMillis();
                report(Action.DELETE, true, (int)(after - before));
                reportError("Error deleting record.", t);
            }

            if (checkStopConditions()) break;
        }
    }

    private void testUpdate() {
        for (int i = 0; i < updateCount; i++) {
            TestRecord testRecord = getNonDeletedRecord();

            if (testRecord == null)
                continue;

            int selectedField = (int)Math.floor(Math.random() * fields.size());
            Field field = fields.get(selectedField);

            Record updatedRecord = testRecord.record.clone();
            updatedRecord.setField(field.fieldType.getName(), field.generateValue());

            long before = System.currentTimeMillis();
            try {
                updatedRecord = repository.update(updatedRecord);
                long after = System.currentTimeMillis();
                report(Action.UPDATE, true, (int)(after - before));

                testRecord.record = updatedRecord;
            } catch (Throwable t) {
                long after = System.currentTimeMillis();
                report(Action.UPDATE, true, (int)(after - before));
                reportError("Error updating record.", t);
            }

            if (checkStopConditions()) break;
        }
    }

    private void testRead() {
        for (int i = 0; i < readCount; i++) {
            TestRecord testRecord = getNonDeletedRecord();

            if (testRecord == null)
                continue;

            long before = System.currentTimeMillis();
            try {
                Record readRecord = repository.read(testRecord.record.getId());
                long after = System.currentTimeMillis();
                report(Action.READ, true, (int)(after - before));

                if (!readRecord.equals(testRecord.record)) {
                    System.out.println("Read record does not match written record!");
                }
            } catch (Throwable t) {
                long after = System.currentTimeMillis();
                report(Action.READ, true, (int)(after - before));
                reportError("Error reading record.", t);
            }

            if (checkStopConditions()) break;
        }
    }

    private void testCreate() {
        for (int i = 0; i < createCount; i++) {
            long before = System.currentTimeMillis();
            try {
                Record record = repository.newRecord();
                record.setRecordType(recordType.getName());
                for (Field field : fields) {
                    record.setField(field.fieldType.getName(), field.generateValue());
                }

                before = System.currentTimeMillis();
                record = repository.create(record);
                long after = System.currentTimeMillis();
                report(Action.CREATE, true, (int)(after - before));
                records.add(new TestRecord(record));
            } catch (Throwable t) {
                long after = System.currentTimeMillis();
                report(Action.CREATE, false, (int)(after - before));
                reportError("Error creating record.", t);
            }

            if (checkStopConditions()) break;
        }
    }

    private boolean checkStopConditions() {
        if (failureCount >= maximumFailures) {
            System.out.println("Stopping because maximum number of failures is reached: " + maximumFailures);
            return true;
        }

        if (records.size() >= maximumRecordCreated) {
            System.out.println("Stopping because maximum number of records is reached: " + maximumRecordCreated);
            return true;
        }

        int ran = (int)Math.floor((System.currentTimeMillis() - startTime) / 1000 / 60);
        if (ran >= maximumRunTime) {
            System.out.println("Stopping because maximum running time is reached: " + maximumRunTime + " minutes.");
            return true;
        }

        return false;
    }

    private TestRecord getNonDeletedRecord() {
        if (records.size() == 0) {
            return null;
        }

        TestRecord testRecord;
        int loopCnt = 0;
        do {
            int selectedIndex = (int)Math.floor(Math.random() * records.size());
            testRecord = records.get(selectedIndex);
            loopCnt++;
            if ((loopCnt % 100) == 0) {
                System.out.println("Already tried " + loopCnt + " times to pick a non-deleted record.");
            }
        } while (testRecord.deleted);

        return testRecord;
    }

    private void report(Action action, boolean success, int duration) {
        reportStream.println(new DateTime() + ","+iteration+"," +(success ? "S" : "F") + "," + action + "," + duration);

        if (metrics != null) {
            metrics.report(action, success, duration);
        }
        if (durationMetrics != null) {
            durationMetrics.increment(action.name(), duration);
        }
    }

    private void reportError(String message, Throwable throwable) {
        failureCount++;
        errorStream.println("[" + new DateTime() + "] " + message);
        StackTracePrinter.printStackTrace(throwable, errorStream);
        errorStream.println("---------------------------------------------------------------------------");        
    }

    private class Field {
        FieldType fieldType;

        public Field(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        public Object generateValue() {
            return generateMultiValue();
        }

        private Object generateMultiValue() {
            if (fieldType.getValueType().isMultiValue()) {
                int size = (int)Math.ceil(Math.random() * 5);
                List<Object> values = new ArrayList<Object>();
                for (int i = 0; i < size; i++) {
                    values.add(generateHierarchical());
                }
                return values;
            } else {
                return generateHierarchical();
            }
        }

        private Object generateHierarchical() {
            if (fieldType.getValueType().isHierarchical()) {
                int size = (int)Math.ceil(Math.random() * 5);
                Object[] elements = new Object[size];
                for (int i = 0; i < size; i++) {
                    elements[i] = generatePrimitiveValue();
                }
                return new HierarchyPath(elements);
            } else {
                return generatePrimitiveValue();
            }
        }

        private Object generatePrimitiveValue() {
            String primitive = fieldType.getValueType().getPrimitive().getName();

            if (primitive.equals("STRING")) {
                int wordCount = (int)Math.floor(Math.random() * 100);
                return loremIpsum.getWords(wordCount);
            } else if (primitive.equals("INTEGER")) {
                double value = Math.floor(Math.random() * Integer.MAX_VALUE * 2);
                return (int)(value - Integer.MAX_VALUE);
            } else if (primitive.equals("LONG")) {
                double value = (long)Math.floor(Math.random() * Long.MAX_VALUE * 2);
                return (long)(value - Long.MAX_VALUE);
            } else if (primitive.equals("BOOLEAN")) {
                int value = (int)Math.floor(Math.random() * 1);
                return value != 0;
            } else if (primitive.equals("DATE")) {
                int year = 1950 + (int)(Math.random() * 100);
                int month = (int)Math.ceil(Math.random() * 12);
                int day = (int)Math.ceil(Math.random() * 25);
                return new LocalDate(year, month, day);
            } else if (primitive.equals("DATETIME")) {
                return generateDateTime();
            } else if (primitive.equals("LINK")) {
                return new Link(repository.getIdGenerator().newRecordId());
            } else {
                throw new RuntimeException("Unsupported primitive value type: " + primitive);
            }
        }

        private DateTime generateDateTime() {
            int fail = 0;
            while (true) {
                int year = 1950 + (int)(Math.random() * 100);
                int month = (int)Math.ceil(Math.random() * 12);
                int day = (int)Math.ceil(Math.random() * 25);
                int hour = (int)Math.floor(Math.random() * 24);
                int minute = (int)Math.floor(Math.random() * 60);
                int second = (int)Math.floor(Math.random() * 60);
                try {
                    return new DateTime(year, month, day, hour, minute, second, 0);
                } catch (IllegalArgumentException e) {
                    // We can get exceptions here of the kind:
                    //  "Illegal instant due to time zone offset transition"
                    // This can occur if we happen to generate a time which falls in daylight
                    // saving.
                    if (fail > 10) {
                        throw new RuntimeException("Strange: did not succeed to generate a valid date after "
                                + fail + " tries.", e);
                    }
                    fail++;
                }
            }
        }
    }

    private static final class TestRecord {
        Record record;
        boolean deleted;

        public TestRecord(Record record) {
            this.record = record;
        }
    }
}
