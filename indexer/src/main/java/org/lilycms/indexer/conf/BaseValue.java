package org.lilycms.indexer.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.lilycms.indexer.formatter.Formatter;
import org.lilycms.repository.api.*;
import org.lilycms.util.io.Closer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseValue implements Value {
    private Formatters formatters;
    private String formatterName;
    private boolean extractContent;

    private Log log = LogFactory.getLog(getClass());

    public BaseValue(boolean extractContent, String formatterName, Formatters formatters) {
        this.extractContent = extractContent;
        this.formatterName = formatterName;
        this.formatters = formatters;
    }

    public List<String> eval(IdRecord record, Repository repository, String vtag) {
        Object value = evalInt(record, repository, vtag);
        if (value == null)
            return null;

        if (extractContent) {
            return extractContent(value, record, repository);
        }

        ValueType valueType = getValueType();
        Formatter formatter = formatterName != null ? formatters.getFormatter(formatterName) :formatters.getFormatter(valueType);

        return formatter.format(value, valueType);
    }

    private List<String> extractContent(Object value, IdRecord record, Repository repository) {
        // At this point we can be sure the value will be a blob, this is validated during
        // the construction of the indexer conf.

        ValueType valueType = getValueType();

        List<Blob> blobs = new ArrayList<Blob>();
        collectBlobs(value, valueType, blobs);

        if (blobs.size() == 0)
            return null;

        List<String> result = new ArrayList<String>(blobs.size());

        Parser parser = new AutoDetectParser();

        // TODO add some debug (or even info) logging to indicate what we are working on.
        for (Blob blob : blobs) {
            InputStream is = null;
            try {
                is = repository.getInputStream(blob);

                // TODO make write limit configurable
                BodyContentHandler ch = new BodyContentHandler();

                Metadata metadata = new Metadata();
                metadata.add(Metadata.CONTENT_TYPE, blob.getMimetype());
                if (blob.getName() != null)
                    metadata.add(Metadata.RESOURCE_NAME_KEY, blob.getName());

                ParseContext parseContext = new ParseContext();

                parser.parse(is, ch, metadata, parseContext);

                String text = ch.toString();
                if (text.length() > 0)
                    result.add(text);

            } catch (Throwable t) {
                log.error("Error extracting blob content. Field: " + getTargetFieldType().getName() + ", record: "
                        + record.getId(), t);
            } finally {
                Closer.close(is);
            }
        }

        return result.isEmpty() ? null : result;
    }

    private void collectBlobs(Object value, ValueType valueType, List<Blob> blobs) {
        if (valueType.isMultiValue()) {
            List values = (List)value;
            for (Object item : values)
                collectBlobsHierarchical(item, valueType, blobs);
        } else {
            collectBlobsHierarchical(value, valueType, blobs);
        }
    }

    private void collectBlobsHierarchical(Object value, ValueType valueType, List<Blob> blobs) {
        if (valueType.isHierarchical()) {
            HierarchyPath hierarchyPath = (HierarchyPath)value;
            for (Object item : hierarchyPath.getElements())
                blobs.add((Blob)item);
        } else {
            blobs.add((Blob)value);
        }
    }

    public abstract Object evalInt(IdRecord record, Repository repository, String vtag);

    /**
     * Returns the value type of the actual value to index. This does not necessarily correspond
     * to the value type of the field type returned by {@link #getTargetFieldType()} since it
     * might be 'corrected' to multi-value in case of multi-value link field dereferencing.
     */
    public abstract ValueType getValueType();

    /**
     * Get the FieldType of the field from which the actual data is taken, thus in case
     * of a dereference the last field in the chain.
     */
    public abstract FieldType getTargetFieldType();
}
