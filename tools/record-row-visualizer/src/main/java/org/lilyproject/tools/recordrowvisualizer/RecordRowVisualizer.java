package org.lilyproject.tools.recordrowvisualizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.kauriproject.template.*;
import org.kauriproject.template.source.ClasspathSourceResolver;
import org.kauriproject.template.source.Source;
import org.kauriproject.template.source.SourceResolver;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.rowlog.impl.SubscriptionExecutionState;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.xml.sax.SAXException;

import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * Tool to visualize the HBase-storage structure of a Lily record, in the form
 * of an HTML page.
 */
public class RecordRowVisualizer extends BaseZkCliTool {
    protected Option recordIdOption;
    protected RecordRow recordRow;
    protected TypeManager typeMgr;

    @Override
    protected String getCmdName() {
        return "lily-record-row";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        recordIdOption = OptionBuilder
                .withArgName("record-id")
                .isRequired()
                .hasArg()
                .withDescription("A Lily record ID: UUID.something or USER.something")
                .withLongOpt("record-id")
                .create("r");

        options.add(recordIdOption);

        return options;
    }

    public static void main(String[] args) {
        new RecordRowVisualizer().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result =  super.run(cmd);
        if (result != 0)
            return result;

        String recordIdString = cmd.getOptionValue(recordIdOption.getOpt());

        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.fromString(recordIdString);

        recordRow = new RecordRow();
        recordRow.recordId = recordId;


        // HBase record table
        Configuration conf = HBaseConfiguration.create();
        HTableInterface table = new HTable(conf, Table.RECORD.bytes);

        // Type manager
        // TODO should be able to avoid ZK for this use-case?
        final ZooKeeperItf zk = new StateWatchingZooKeeper(zkConnectionString, 10000);
        typeMgr = new HBaseTypeManager(idGenerator, conf, zk);

        Get get = new Get(recordId.toBytes());
        get.setMaxVersions();
        Result row = table.get(get);

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long,byte[]>>> root = row.getMap();

        readNonVersionedSystemFields(root.get(RecordCf.NON_VERSIONED_SYSTEM.bytes));
        readNonVersionedFields(root.get(RecordCf.NON_VERSIONED.bytes));

        readVersionedSystemFields(root.get(RecordCf.VERSIONED_SYSTEM.bytes));
        readVersionedFields(root.get(RecordCf.VERSIONED.bytes));

        readExecutionState(recordRow.mqState, root.get(RecordCf.MQ_STATE.bytes));
        readPayload(recordRow.mqPayload, root.get(RecordCf.MQ_PAYLOAD.bytes));

        readExecutionState(recordRow.walState, root.get(RecordCf.WAL_STATE.bytes));
        readPayload(recordRow.walPayload, root.get(RecordCf.WAL_PAYLOAD.bytes));

        byte[][] treatedColumnFamilies = {
                RecordCf.NON_VERSIONED_SYSTEM.bytes,
                RecordCf.VERSIONED_SYSTEM.bytes,
                RecordCf.NON_VERSIONED.bytes,
                RecordCf.VERSIONED.bytes,
                RecordCf.MQ_STATE.bytes,
                RecordCf.MQ_PAYLOAD.bytes,
                RecordCf.WAL_STATE.bytes,
                RecordCf.WAL_PAYLOAD.bytes
        };

        for (byte[] cf : root.keySet()) {
            if (!isInArray(cf, treatedColumnFamilies)) {
                recordRow.unknownColumnFamilies.add(Bytes.toString(cf));
            }
        }

        executeTemplate("org/lilyproject/tools/recordrowvisualizer/recordrow2html.xml",
                Collections.<String, Object>singletonMap("row", recordRow), System.out);

        return 0;
    }

    private boolean isInArray(byte[] key, byte[][] data) {
        for (byte[] item : data) {
            if (Arrays.equals(item, key))
                return true;
        }
        return false;
    }

    private void readNonVersionedSystemFields(NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws Exception {
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : cf.entrySet()) {
            byte[] columnKey = columnEntry.getKey();

            if (Arrays.equals(columnKey, RecordColumn.DELETED.bytes)) {
                setVersionedValue(recordRow.deleted, columnEntry.getValue(), BOOLEAN_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.NON_VERSIONED_RT_ID.bytes)) {
                setTypeId(recordRow.nvRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.NON_VERSIONED_RT_VERSION.bytes)) {
                setTypeVersion(recordRow.nvRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.LOCK.bytes)) {
                setVersionedValue(recordRow.lock, columnEntry.getValue(), BASE64_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.VERSION.bytes)) {
                setVersionedValue(recordRow.version, columnEntry.getValue(), LONG_DECODER);
            } else {
                recordRow.unknownNvColumns.add(Bytes.toString(columnKey));
            }
        }

        for (Type type : recordRow.nvRecordType.getValues().values()) {
            type.object = typeMgr.getRecordTypeById(type.getId(), type.getVersion());
        }
    }

    private void readVersionedSystemFields(NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws Exception {
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : cf.entrySet()) {
            byte[] columnKey = columnEntry.getKey();

            if (Arrays.equals(columnKey, RecordColumn.VERSIONED_RT_ID.bytes)) {
                setTypeId(recordRow.vRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_RT_VERSION.bytes)) {
                setTypeVersion(recordRow.vRecordType, columnEntry.getValue());
            } else {
                recordRow.unknownVColumns.add(Bytes.toString(columnKey));
            }
        }

        for (Type type : recordRow.vRecordType.getValues().values()) {
            type.object = typeMgr.getRecordTypeById(type.getId(), type.getVersion());
        }
    }

    private void readVersionedFields(NavigableMap<byte[], NavigableMap<Long,byte[]>> cf) throws Exception {
        if (cf == null)
            return;

        recordRow.vFields = new Fields(cf, typeMgr);
    }

    private void readNonVersionedFields(NavigableMap<byte[], NavigableMap<Long,byte[]>> cf) throws Exception {
        if (cf == null)
            return;

        recordRow.nvFields = new Fields(cf, typeMgr);
    }

    private void readExecutionState(Map<RowLogKey, List<ExecutionData>> stateByKey, NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws IOException {
        if (cf == null)
            return;
        // columns are long seqnr as bytes
        // value is SubscriptionExecutionState as bytes


        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> rowEntry : cf.entrySet()) {
            byte[] column = rowEntry.getKey();
            long seqNr = Bytes.toLong(column);

            for (Map.Entry<Long, byte[]> columnEntry : rowEntry.getValue().entrySet()) {
                RowLogKey key = new RowLogKey(seqNr, columnEntry.getKey());

                List<ExecutionData> states = stateByKey.get(key);
                if (states == null) {
                    states = new ArrayList<ExecutionData>();
                    stateByKey.put(key, states);
                }

                SubscriptionExecutionState state = SubscriptionExecutionState.fromBytes(columnEntry.getValue());
                for (CharSequence subscriptionIdCharSeq : state.getSubscriptionIds()) {
                    String subscriptionId = subscriptionIdCharSeq.toString();

                    ExecutionData data = new ExecutionData();
                    data.subscriptionId = subscriptionId;
                    data.tryCount = state.getTryCount(subscriptionId);
                    data.success = state.getState(subscriptionId);
                    data.lock = BASE64_DECODER.decode(state.getLock(subscriptionId));
                    states.add(data);
                }

            }
        }
    }

    // TODO define in LilyHBaseSchema?
    private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");

    public void readPayload(Map<RowLogKey, List<String>> payloadByKey, NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws IOException {
        if (cf == null)
            return;

        NavigableMap<Long, byte[]> maxSeqNr = null;

        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> rowEntry : cf.entrySet()) {
            byte[] column = rowEntry.getKey();

            if (Arrays.equals(column, SEQ_NR)) {
                maxSeqNr = rowEntry.getValue();
            } else {
                long seqNr = Bytes.toLong(column);

                for (Map.Entry<Long, byte[]> columnEntry : rowEntry.getValue().entrySet()) {
                    RowLogKey key = new RowLogKey(seqNr, columnEntry.getKey());
                    List<String> payloads = payloadByKey.get(key);
                    if (payloads == null) {
                        payloads = new ArrayList<String>();
                        payloadByKey.put(key, payloads);
                    }

                    payloads.add(new String(columnEntry.getValue(), "UTF-8"));
                }
            }
        }

        if (maxSeqNr != null) {
            // TODO
        }
    }

    private void setVersionedValue(VersionedValue value, NavigableMap<Long, byte[]> valuesByVersion, ValueDecoder decoder) {
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            value.put(entry.getKey(), decoder.decode(entry.getValue()));
        }
    }

    private void setTypeId(VersionedValue value, NavigableMap<Long, byte[]> valuesByVersion) {
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            Type type = (Type)value.get(entry.getKey());
            if (type != null) {
                type.id = STRING_DECODER.decode(entry.getValue());
            } else {
                type = new Type();
                type.id = STRING_DECODER.decode(entry.getValue());
                value.put(entry.getKey(), type);
            }
        }
    }

    private void setTypeVersion(VersionedValue value, NavigableMap<Long, byte[]> valuesByVersion) {
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            Type type = (Type)value.get(entry.getKey());
            if (type != null) {
                type.version = LONG_DECODER.decode(entry.getValue());
            } else {
                type = new Type();
                type.version = LONG_DECODER.decode(entry.getValue());
                value.put(entry.getKey(), type);
            }
        }
    }

    public static interface ValueDecoder<T> {
        T decode(byte[] bytes);
    }

    public static class LongValueDecoder implements ValueDecoder<Long> {
        public Long decode(byte[] bytes) {
            return Bytes.toLong(bytes);
        }
    }

    public static class BooleanValueDecoder implements ValueDecoder<Boolean> {
        public Boolean decode(byte[] bytes) {
            return Bytes.toBoolean(bytes);
        }
    }

    public static class StringValueDecoder implements ValueDecoder<String> {
        public String decode(byte[] bytes) {
            return Bytes.toString(bytes);
        }
    }

    public static class Base64ValueDecoder implements ValueDecoder<String> {
        public String decode(byte[] bytes) {
            char[] result = new char[bytes.length * 2];

            for (int i = 0; i < bytes.length; i++) {
                byte ch = bytes[i];
                result[2 * i] = Character.forDigit(Math.abs(ch >> 4), 16);
                result[2 * i + 1] = Character.forDigit(Math.abs(ch & 0x0f), 16);
            }

            return new String(result);
        }
    }

    private static final StringValueDecoder STRING_DECODER = new StringValueDecoder();
    private static final BooleanValueDecoder BOOLEAN_DECODER = new BooleanValueDecoder();
    private static final LongValueDecoder LONG_DECODER = new LongValueDecoder();
    private static final Base64ValueDecoder BASE64_DECODER = new Base64ValueDecoder();

    private void executeTemplate(String template, Map<String, Object> variables, OutputStream os) throws SAXException {
        DefaultTemplateBuilder builder = new DefaultTemplateBuilder(null, new DefaultTemplateService(), false);
        SourceResolver sourceResolver = new ClasspathSourceResolver();
        Source resource = sourceResolver.resolve(template);
        CompiledTemplate compiledTemplate = builder.buildTemplate(resource);

        TemplateContext context = new DefaultTemplateContext();
        context.putAll(variables);

        ExecutionContext execContext = new ExecutionContext();
        execContext.setTemplateContext(context);
        execContext.setSourceResolver(sourceResolver);
        DefaultTemplateExecutor executor = new DefaultTemplateExecutor();

        TemplateResult result = new TemplateResultImpl(new KauriSaxHandler(os, KauriSaxHandler.OutputFormat.HTML, "UTF-8"));
        executor.execute(compiledTemplate, null, execContext, result);
        result.flush();
    }

}
