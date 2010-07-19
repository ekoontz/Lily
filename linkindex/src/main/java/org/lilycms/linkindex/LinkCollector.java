package org.lilycms.linkindex;

import org.lilycms.repository.api.RecordId;

import java.util.HashSet;
import java.util.Set;

public class LinkCollector {
    private Set<FieldedLink> links = new HashSet<FieldedLink>();

    public void addLink(RecordId target, String fieldTypeId) {
        links.add(new FieldedLink(target, fieldTypeId));
    }

    public Set<FieldedLink> getLinks() {
        return links;
    }
}
