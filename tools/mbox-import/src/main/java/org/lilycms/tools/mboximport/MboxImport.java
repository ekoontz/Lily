package org.lilycms.tools.mboximport;

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
import org.apache.james.mime4j.parser.MimeTokenStream;
import org.apache.james.mime4j.util.MimeUtil;
import org.lilycms.client.LilyClient;
import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.cli.JsonImportTool;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MboxImport {
    private static final String NS = "org.lilycms.mail";

    public static void main(String[] args) throws Exception {
        new MboxImport().run(args);
    }

    public void run(String[] args) throws Exception {
        LilyClient lilyClient = new LilyClient("localhost:2181");
        loadSchema(lilyClient);


        InputStream is = new BufferedInputStream(new FileInputStream(args[0]));
        MboxInputStream mboxStream = new MboxInputStream(is);

        while (mboxStream.nextMessage()) {
            MimeTokenStream stream = new MimeTokenStream();
            stream.parse(new BufferedInputStream(mboxStream));
            importMessage(stream, lilyClient.getRepository());
        }
    }

    private void loadSchema(LilyClient lilyClient) throws Exception {
        Repository repository = lilyClient.getRepository();
        InputStream is = getClass().getClassLoader().getResourceAsStream("org/lilycms/tools/mboximport/mail_schema.json");
        JsonImportTool.load(repository, is, false);
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

                    message.addPart(blob);

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
        messageRecord = repository.create(messageRecord);

        for (Part part : message.parts) {
            Record partRecord = repository.newRecord();
            partRecord.setRecordType(new QName(NS, "Part"));
            partRecord.setField(new QName(NS, "mimeType"), part.blob.getMimetype());
            partRecord.setField(new QName(NS, "content"), part.blob);
            partRecord.setField(new QName(NS, "message"), new Link(messageRecord.getId()));
            partRecord = repository.create(partRecord);
            System.out.println("Created part record: " + partRecord.getId());
            part.recordId = partRecord.getId();
        }

        List<Link> partLinks = new ArrayList<Link>(message.parts.size());
        for (Part part : message.parts) {
            partLinks.add(new Link(part.recordId));
        }

        messageRecord.setField(new QName(NS, "parts"), partLinks);
        repository.update(messageRecord);

        System.out.println("Created message record " + messageRecord.getId());
    }

    private static class Message {
        public String subject;
        public AddressList to;
        public AddressList cc;
        public MailboxList from;
        public Mailbox sender;
        public String listId;

        public List<Part> parts = new ArrayList<Part>();

        public void addPart(Blob blob) {
            Part part = new Part();
            part.blob = blob;
            parts.add(part);
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
    }
}
