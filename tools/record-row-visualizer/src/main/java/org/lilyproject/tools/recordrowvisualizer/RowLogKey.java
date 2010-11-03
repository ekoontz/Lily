package org.lilyproject.tools.recordrowvisualizer;

public class RowLogKey implements Comparable<RowLogKey> {
    private long sequenceNr;
    private long hbaseVersion;

    public RowLogKey(long sequenceNr, long hbaseVersion) {
        this.sequenceNr = sequenceNr;
        this.hbaseVersion = hbaseVersion;
    }

    public long getSequenceNr() {
        return sequenceNr;
    }

    public long getHbaseVersion() {
        return hbaseVersion;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowLogKey other = (RowLogKey) obj;
        return other.sequenceNr == sequenceNr && other.hbaseVersion == hbaseVersion;
    }

    @Override
    public int hashCode() {
        return (int)(sequenceNr + hbaseVersion);
    }

    public int compareTo(RowLogKey o) {
        if (sequenceNr < o.sequenceNr)
            return -1;
        else if (sequenceNr > o.sequenceNr)
            return 1;

        if (hbaseVersion < o.hbaseVersion)
            return -1;
        else if (hbaseVersion > o.hbaseVersion)
            return 1;

        return 0;
    }
}
