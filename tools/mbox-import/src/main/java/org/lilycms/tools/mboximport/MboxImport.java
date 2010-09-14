package org.lilycms.tools.mboximport;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.james.mime4j.codec.Base64InputStream;
import org.apache.james.mime4j.codec.QuotedPrintableInputStream;
import org.apache.james.mime4j.field.*;
import org.apache.james.mime4j.field.address.Address;
import org.apache.james.mime4j.field.address.AddressList;
import org.apache.james.mime4j.field.address.Mailbox;
import org.apache.james.mime4j.field.address.MailboxList;
import org.apache.james.mime4j.io.EOLConvertingInputStream;
import org.apache.james.mime4j.parser.Field;
import org.apache.james.mime4j.parser.MimeEntityConfig;
import org.apache.james.mime4j.parser.MimeTokenStream;
import org.apache.james.mime4j.util.MimeUtil;
import org.lilycms.client.LilyClient;
import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.cli.JsonImportTool;
import org.lilycms.util.io.Closer;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class MboxImport {

    private LilyClient lilyClient;

    private int messageCount;

    private int partCount;

    private int invalidMessageCount;

    private Map<String, Integer> partsByMediaType = new HashMap<String, Integer>();

    private static final String NS = "org.lilycms.mail";

    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";

    public static void main(String[] args) throws Exception {
        new MboxImport().run(args);
    }

    public void run(String[] args) throws Exception {
        Options cliOptions = new Options();

        Option helpOption = new Option("h", "help", false, "Shows help");
        cliOptions.addOption(helpOption);

        Option fileOption = OptionBuilder
                .withArgName("file")
                .hasArg()
                .withDescription("File or directory name")
                .withLongOpt("file")
                .create("f");
        cliOptions.addOption(fileOption);

        Option zkOption = OptionBuilder
                .withArgName("connection-string")
                .hasArg()
                .withDescription("Zookeeper connection string: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");
        cliOptions.addOption(zkOption);


        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        boolean showHelp = false;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            showHelp = true;
        }

        if (showHelp || cmd.hasOption(helpOption.getOpt())) {
            printHelp(cliOptions);
            System.exit(1);
        }

        String zkConnectionString;
        if (!cmd.hasOption(zkOption.getOpt())) {
            System.out.println("Zookeeper connection string not specified, using default: " + DEFAULT_ZK_CONNECT);
            zkConnectionString = DEFAULT_ZK_CONNECT;
        } else {
            zkConnectionString = cmd.getOptionValue(zkOption.getOpt());
        }

        lilyClient = new LilyClient(zkConnectionString);

        loadSchema();

        if (cmd.hasOption(fileOption.getOpt())) {
            try {
                String fileName = cmd.getOptionValue(fileOption.getOpt());
                File file = new File(fileName);

                if (!file.exists()) {
                    System.out.println("File does not exist: " + file.getAbsolutePath());
                    System.exit(1);
                }

                if (file.isDirectory()) {
                    File[] files = file.listFiles();
                    Arrays.sort(files);
                    for (File item : files) {
                        if (!item.isDirectory()) {
                            importFile(item);
                        }
                    }
                } else {
                    importFile(file);
                }

            } finally {
                System.out.println();
                System.out.println("Number of created messages: " + messageCount);
                System.out.println("Number of created parts: " + partCount);
                System.out.println("Number of invalid messages (msg without headers or parts): " + invalidMessageCount);
                System.out.println();
                System.out.println("Number of created parts per media type:");
                for (Map.Entry<String, Integer> entry : partsByMediaType.entrySet()) {
                    System.out.println("  " + entry.getKey() + " : " + entry.getValue());
                }
            }
        }

    }

    private void printHelp(Options cliOptions) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("lily-mbox-import", cliOptions, true);
    }

    private void loadSchema() throws Exception {
        System.out.println("Creating the schema (if necessary)");
        System.out.println();
        Repository repository = lilyClient.getRepository();
        InputStream is = getClass().getClassLoader().getResourceAsStream("org/lilycms/tools/mboximport/mail_schema.json");
        JsonImportTool.load(repository, is, false);
        System.out.println();
    }

    private void importFile(File file) throws Exception {
        System.out.println("Processing file " + file.getAbsolutePath());
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));

            if (file.getName().endsWith(".gz")) {
                is = new GZIPInputStream(is);
            }

            MboxInputStream mboxStream = new MboxInputStream(is);

            while (mboxStream.nextMessage()) {
                MimeTokenStream stream = new MyMimeTokenStream();
                stream.parse(new BufferedInputStream(mboxStream));
                importMessage(stream, lilyClient.getRepository());
            }

        } finally {
            Closer.close(is);
        }
        System.out.println();
    }

    public static class MyMimeTokenStream extends MimeTokenStream {
        protected MyMimeTokenStream() {
            super(getConfig());
        }

        private static MimeEntityConfig getConfig() {
            MimeEntityConfig config = new MimeEntityConfig();
            config.setMaxLineLen(10000);
            return config;
        }
    }

    private void importMessage(MimeTokenStream stream, Repository repository) throws Exception {
        int multiPartNesting = 0; // note that a multipart can again contain a multipart

        Message message = new Message();

        for (int state = stream.getState();
             state != MimeTokenStream.T_END_OF_STREAM;
             state = stream.next()) {

            switch (state) {
                case MimeTokenStream.T_BODY:
                    String mediaType = stream.getBodyDescriptor().getMimeType() + "; charset=" + stream.getBodyDescriptor().getCharset();

                    // oftwewel: gebruik getDecodedInputStream
                    InputStream bodyDataStream;
                    if (MimeUtil.isQuotedPrintableEncoded(stream.getBodyDescriptor().getTransferEncoding())) {
                        bodyDataStream = new QuotedPrintableInputStream(new EOLConvertingInputStream(stream.getInputStream(), EOLConvertingInputStream.CONVERT_LF));
                    } else if (MimeUtil.isBase64Encoding(stream.getBodyDescriptor().getTransferEncoding())) {
                        bodyDataStream = new Base64InputStream(stream.getInputStream());
                    } else {
                        bodyDataStream = stream.getInputStream();
                    }

                    byte[] data = IOUtils.toByteArray(bodyDataStream);

                    // TODO could fill in filename
                    Blob blob = new Blob(mediaType, (long)data.length, null);
                    OutputStream os = repository.getOutputStream(blob);
                    try {
                        IOUtils.write(data, os);
                    } finally {
                        os.close();
                    }

                    Part part = message.addPart(blob);
                    part.baseMediaType = stream.getBodyDescriptor().getMimeType();

                    break;
                case MimeTokenStream.T_FIELD:
                    if (multiPartNesting == 0) {
                        Field field = stream.getField();
                        ParsedField parsedField = new DefaultFieldParser().parse(field.getName(), MimeUtil.unfold(field.getBody()), null);
                        if (parsedField.getParseException() != null) {
                            // TODO print error
                        } else if (parsedField.getName().equals(FieldName.TO)) {
                            message.to = ((AddressListField)parsedField).getAddressList();
                        } else if (parsedField.getName().equals(FieldName.CC)) {
                            message.cc = ((AddressListField)parsedField).getAddressList();
                        } else if (parsedField.getName().equals(FieldName.FROM)) {
                            message.from = ((MailboxListField)parsedField).getMailboxList();
                        } else if (parsedField.getName().equals(FieldName.SENDER)) {
                            message.sender = ((MailboxField)parsedField).getMailbox();
                        } else if (parsedField.getName().equals("List-Id")) {
                            message.listId = parsedField.getBody();
                        } else if (parsedField.getName().equals(FieldName.SUBJECT)) {
                            message.subject = parsedField.getBody();
                        }
                    }
                    break;
                case MimeTokenStream.T_START_MULTIPART:
                    multiPartNesting++;
                    break;
                case MimeTokenStream.T_END_MULTIPART:
                    multiPartNesting--;
            }
        }

        // Now create the records in Lily
        Record messageRecord = repository.newRecord();
        messageRecord.setRecordType(new QName(NS, "Message"));
        if (message.subject != null)
            messageRecord.setField(new QName(NS, "subject"), message.subject);
        if (message.to != null)
            messageRecord.setField(new QName(NS, "to"), message.getToAddressesAsStringList());
        if (message.cc != null)
            messageRecord.setField(new QName(NS, "cc"), message.getCcAddressesAsStringList());
        if (message.from != null)
            messageRecord.setField(new QName(NS, "from"), message.getFromAddressesAsStringList());
        if (message.sender != null)
            messageRecord.setField(new QName(NS, "sender"), message.getSenderAddressAsString());
        if (message.listId != null)
            messageRecord.setField(new QName(NS, "listId"), message.listId);

        if (messageRecord.getFields().size() == 0 || message.parts.size() == 0) {
            // Message has no useful headers, do not create it.
            invalidMessageCount++;
            return;
        }

        messageRecord = repository.create(messageRecord);
        messageCount++;

        for (Part part : message.parts) {
            Record partRecord = repository.newRecord();
            partRecord.setRecordType(new QName(NS, "Part"));
            partRecord.setField(new QName(NS, "mediaType"), part.blob.getMimetype());
            partRecord.setField(new QName(NS, "content"), part.blob);
            partRecord.setField(new QName(NS, "message"), new Link(messageRecord.getId()));
            partRecord = repository.create(partRecord);
            part.recordId = partRecord.getId();
            increment(part.baseMediaType);
            partCount++;

            System.out.println("Created part record: " + partRecord.getId());
        }

        List<Link> partLinks = new ArrayList<Link>(message.parts.size());
        for (Part part : message.parts) {
            partLinks.add(new Link(part.recordId));
        }

        messageRecord.setField(new QName(NS, "parts"), partLinks);
        repository.update(messageRecord);

        System.out.println("Created message record " + messageRecord.getId());
    }

    public void increment(String mediaType) {
        Integer count = partsByMediaType.get(mediaType);
        if (count == null) {
            partsByMediaType.put(mediaType, 1);
        } else {
            partsByMediaType.put(mediaType, count + 1);
        }
    }

    private static class Message {
        public String subject;
        public AddressList to;
        public AddressList cc;
        public MailboxList from;
        public Mailbox sender;
        public String listId;

        public List<Part> parts = new ArrayList<Part>();

        public Part addPart(Blob blob) {
            Part part = new Part();
            part.blob = blob;
            parts.add(part);
            return part;
        }

        public List<String> getToAddressesAsStringList() {
            List<String> result = new ArrayList<String>(to.size());
            for (Address address : to) {
                result.add(address.getDisplayString());
            }
            return result;
        }

        public List<String> getCcAddressesAsStringList() {
            List<String> result = new ArrayList<String>(cc.size());
            for (Address address : cc) {
                result.add(address.getDisplayString());
            }
            return result;
        }

        public List<String> getFromAddressesAsStringList() {
            List<String> result = new ArrayList<String>(from.size());
            for (Mailbox mailbox : from) {
                result.add(mailbox.getDisplayString());
            }
            return result;
        }

        public String getSenderAddressAsString() {
            return sender.getDisplayString();
        }
    }

    private static class Part {
        public Blob blob;
        public RecordId recordId;
        /** Media type without parameters. */
        public String baseMediaType;
    }
}
