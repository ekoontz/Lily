package org.lilycms.rest.json;

import java.util.HashMap;
import java.util.Map;

public class Namespaces {
    // Each prefix can be used for only one namespace, and each namespace can be bound to only one prefix.
    private Map<String, String> nsToPrefix = new HashMap<String, String>();
    private Map<String, String> prefixToNs = new HashMap<String, String>();
    private int counter = 0;

    public String getOrMakePrefix(String namespace) {
        String prefix = nsToPrefix.get(namespace);
        if (prefix == null) {
            counter++;
            prefix = "ns" + counter;

            // This assumes we are the only one putting entries in here of the form ns+counter, otherwise
            // we would need to check if it is not already present.
            nsToPrefix.put(namespace, prefix);
            prefixToNs.put(prefix, namespace);
        }
        return prefix;
    }

    public void addMapping(String prefix, String namespace) {
        if (nsToPrefix.containsKey(namespace) && !nsToPrefix.get(namespace).equals(prefix)) {
            throw new RuntimeException("Namespace is already bound to another prefix. Namespace: " + namespace +
                    ", existing prefix: " + nsToPrefix.get(namespace) + ", requested prefix: " + prefix);
        }

        if (prefixToNs.containsKey(prefix) && !prefixToNs.get(prefix).equals(namespace)) {
            throw new RuntimeException("Prefix is already bound to another namespace. Prefix: " + prefix +
                    ", existing namespace: " + prefixToNs.get(prefix) + ", requested namespace: " + namespace);
        }

        nsToPrefix.put(namespace, prefix);
        prefixToNs.put(prefix, namespace);
    }

    public String getNamespace(String prefix) {
        return prefixToNs.get(prefix);
    }

    public String getPrefix(String namespace) {
        return nsToPrefix.get(namespace);
    }

    public Map<String, String> getNsToPrefixMapping() {
        return nsToPrefix;
    }
}
