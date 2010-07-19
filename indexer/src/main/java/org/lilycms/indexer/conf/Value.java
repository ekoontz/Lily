package org.lilycms.indexer.conf;

import java.util.List;

import org.lilycms.repository.api.IdRecord;
import org.lilycms.repository.api.Repository;

public interface Value {
    /**
     * Evaluates this value for a given record & vtag.
     *
     * @return null if there is no value
     */
    List<String> eval(IdRecord record, Repository repository, String vtag);

    /**
     * Returns the field that is used from the record when evaluating this value. It is the value that is taken
     * from the current record, thus in the case of a dereference it is the first link field, not the field value
     * taken from the target document.
     */
    String getFieldDependency();

}
