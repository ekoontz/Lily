package org.lilycms.repository.api;

public class VersionNotFoundException extends RepositoryException {

    private final Record record;

    public VersionNotFoundException(Record record) {
        this.record = record;
    }

    public Record getRecord() {
        return record;
    }

    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();
        message.append("Record <");
        message.append(record.getId());
        message.append("> ");
        Long version = record.getVersion();
        if (version != null) {
            message.append("<version:");
            message.append(version);
            message.append(">");
        }
        message.append("not found");
        return message.toString();
    }
}

