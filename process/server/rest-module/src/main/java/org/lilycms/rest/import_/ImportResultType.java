package org.lilycms.rest.import_;

public enum ImportResultType {
    CREATED(true),
    UPDATED(true),
    CONFLICT(false),
    UP_TO_DATE(true),
    CANNOT_CREATE_EXISTS(false),
    CANNOT_UPDATE_DOES_NOT_EXIST(false);

    private boolean isSuccess;

    private ImportResultType(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    /**
     * Does this result represent a success?
     */
    public boolean isSuccess() {
        return isSuccess;
    }
}
