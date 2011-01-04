package org.lilyproject.repository.api;

public class RecordLockedException extends RepositoryException {
    private final RecordId recordId;

    public RecordLockedException(RecordId recordId) {
        this.recordId = recordId;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    @Override
    public String getMessage() {
        return "Failed to lock row for record " + recordId;
    }
}
