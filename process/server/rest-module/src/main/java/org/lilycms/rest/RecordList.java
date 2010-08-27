package org.lilycms.rest;

import org.lilycms.repository.api.Record;

import java.util.List;

public class RecordList {
    private List<Record> records;

    public RecordList(List<Record> records) {
        this.records = records;
    }

    public List<Record> getRecords() {
        return records;
    }
}
