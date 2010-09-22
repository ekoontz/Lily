package org.lilycms.indexer.model.api;

public enum IndexGeneralState {
    /**
     * Indicates this index can be used by clients. This state has no meaning to the indexer itself.
     */
    ACTIVE,

    /**
     * Indicates this index should not be used by clients. This state has no meaning to the indexer itself,
     * for example the index can still be updating or rebuilding while in this state.
     */
    DISABLED,

    /**
     * Indicates a request towards the indexer master to drop this index.
     */
    DELETE_REQUESTED,

    /**
     * Indicates the delete request is being processed.
     */
    DELETING,

    /**
     * Indicates a delete request failed, set again to {@link #DELETE_REQUESTED} to retry.
     */
    DELETE_FAILED;

    public boolean isDeleteState() {
        return this == IndexGeneralState.DELETE_REQUESTED
                || this == IndexGeneralState.DELETING
                || this == IndexGeneralState.DELETE_FAILED;
    }
}
