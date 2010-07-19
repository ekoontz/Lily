package org.lilycms.repository.api;

import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.ObjectUtils;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * A link to another record.
 *
 * <p>The difference between a Link and a RecordId is that a Link can be relative. This means
 * that the link needs to be resolved against the context it occurs in (= the record it occurs in)
 * in order to get the actual id of the record it points to.
 *
 * <h2>About relative links</h2>
 *
 * <p>As an example, suppose we have a record with a variant property 'language=en'. In this
 * record, we want to link to another record, also having the 'language=en'. By using Link,
 * we can specify only the master record ID of the target record, without the 'language=en'
 * property. The 'language=en' will be inherited from the record in which the link occurs.
 * The advantage is that if the link is copied to another record with e.g. 'language=fr',
 * the link will automatically adjust to this context.
 *
 * <p>Links have the following possibilities to specify the relative link:
 *
 * <ul>
 *  <li>the master record id itself is optional, and can be copied (= inherited) from the context.
 *  <li>it is possible to specify that all variant properties from the context should be copied.
 *      This is specified by the "copyAll" property of this object.
 *  <li>it is possible to specify individual properties. For each individual property, you can
 *      specify one of the following:
 *      <ul>
 *       <li>an exact value
 *       <li>copy the value from the context (if any). This allows to selectively copy some variant
 *           properties from the context. This only makes sense when not using copyAll.
 *       <li>remove the value, if it would have been copied from the context. This is useful if you
 *           want to copy all variant properties from the context (not knowing how many there are),
 *           except some of them.
 *      </ul>
 * </ul>
 *
 * <h2>Creating links</h2>
 *
 * <p>If you just want a link to point to an exact record id, use the constructor
 * <tt>new Link(recordId, false)</tt>.
 *
 * <p>To have a link that copies variant properties from the context, use:
 * <tt>new Link(recordId)</tt>.
 *
 * <p>In such cases you might want to consider creating the link based only on the master record id
 *
 * <tt>new Link(recordId.getMaster())</tt>.
 *
 * <p>The Link class is immutable after construction. You have to either pass all properties
 * through constructor arguments, or use the LinkBuilder class obtained via {@link #newBuilder}.
 *
 * <p>Example using LinkBuilder:
 *
 * <pre>
 * RecordId recordId = ...;
 * Link link = Link.newBuilder().recordId(recordId).copyAll(false).copy("dev").set("foo", "bar").create();
 * </pre>
 *
 * <h2>Resolving links to RecordId's</h2>
 *
 * <p>To resolve a link to a RecordId, using the {@link #resolve(RecordId, IdGenerator)} method.
 */
public class Link {
    private RecordId masterRecordId;
    private boolean copyAll = true;
    private SortedMap<String, PropertyValue> variantProps;

    /**
     * A link to self.
     */
    public Link() {
    }

    /**
     * If copyAll is true, a link to self, if copy all is false, a link to the master.
     */
    public Link(boolean copyAll) {
        this.copyAll = copyAll;
    }

    /**
     * An absolute link to the specified recordId. Nothing will be copied from the context
     * when resolving this link.
     */
    public Link(RecordId recordId) {
        this.masterRecordId = recordId != null ? recordId.getMaster() : null;
        variantProps = createVariantProps(recordId);
    }

    /**
     * A relative link to the specified recordId. All variant properties will be copied
     * from the context when resolving this link, except those that would be explicitly
     * specified on the recordId.
     */
    public Link(RecordId recordId, boolean copyAll) {
        this(recordId);
        this.copyAll = copyAll;
    }

    private Link(RecordId masterRecordId, boolean copyAll, SortedMap<String, PropertyValue> props) {
        this.masterRecordId = masterRecordId;
        this.copyAll = copyAll;
        this.variantProps = props;
    }

    private static SortedMap<String, PropertyValue> createVariantProps(RecordId recordId) {
        if (recordId == null)
            return null;
        
        SortedMap<String, PropertyValue> variantProps = null;
        if (!recordId.isMaster()) {
            variantProps = new TreeMap<String, PropertyValue>();
            for (Map.Entry<String, String> entry : recordId.getVariantProperties().entrySet()) {
                variantProps.put(entry.getKey(), new PropertyValue(PropertyMode.SET, entry.getValue()));
            }
        }
        return variantProps;
    }

    /**
     * Parses a link in the syntax produced by {@link #toString()}.
     *
     * <p>An empty string is interpreted as a link-to-self, thus the same as ".".
     *
     * <p>If the same variant property would be specified multiple times, it is its
     * last occurrence which will count.
     *
     * @throws IllegalArgumentException in case of syntax errors in the link.
     */
    public static Link fromString(String link, IdGenerator idGenerator) {
        ArgumentValidator.notNull(link, "link");

        // link to self
        if (link.equals("") || link.equals(".")) {
            return new Link();
        }

        RecordId recordId;
        String variantString;

        if (link.startsWith(".")) {
            recordId = null;
            variantString = link.substring(1);
        } else {
            int firstDotPos = link.indexOf('.');

            if (firstDotPos == -1) {
                throw new IllegalArgumentException("Invalid link, contains no dot: " + link);
            }

            int secondDotPos = link.indexOf('.', firstDotPos + 1);

            String masterIdString;
            if (secondDotPos == -1) {
                masterIdString = link;
                variantString = null;
            } else {
                masterIdString = link.substring(0, secondDotPos);
                variantString = link.substring(secondDotPos + 1);
            }

            recordId = idGenerator.fromString(masterIdString);
        }

        if (variantString == null) {
            return new Link(recordId, true);
        }

        LinkBuilder builder = Link.newBuilder().recordId(recordId);

        argsFromString(variantString, builder, link);

        return builder.create();
    }

    private static void argsFromString(String args, LinkBuilder builder, String link) {
        String[] variantStringParts = args.split(";");
        for (String part : variantStringParts) {
            int eqPos = part.indexOf('=');
            if (eqPos == -1) {
                String thing = part.trim();
                if (thing.equals("*")) {
                    // this is the default, but if users want to make explicit, allow them
                    builder.copyAll(true);
                } else if (thing.equals("!*")) {
                    builder.copyAll(false);
                } else if (thing.startsWith("+") && thing.length() > 1) {
                    builder.copy(thing.substring(1));
                } else if  (thing.startsWith("-") && thing.length() > 1) {
                    builder.remove(thing.substring(1));
                } else {
                    throw new IllegalArgumentException("Invalid link: " + link);
                }
            } else {
                String name = part.substring(0, eqPos).trim();
                String value = part.substring(eqPos + 1).trim();
                if (name.length() == 0 || value.length() == 0) {
                    throw new IllegalArgumentException("Invalid link: " + link);
                }
                builder.set(name, value);
            }
        }
    }

    public RecordId getMasterRecordId() {
        return masterRecordId;
    }

    public boolean copyAll() {
        return copyAll;
    }

    public Map<String, PropertyValue> getVariantProps() {
        return Collections.unmodifiableMap(variantProps);
    }

    /**
     * Creates a string representation of this link.
     *
     * <p>The syntax is:
     *
     * <pre>{recordId}.!*;arg1=val1;+arg2;-arg3<pre>
     *
     * <p>The recordId is optional. Arguments, if any, follow after the . symbol and are separated by ';'
     * symbols. Note that the {recordId} itself also contains a dot to separate the record id type and its
     * actual content (e.g. USER.235523432).
     *
     * <p>The '!*' argument indicates 'not copyAll', copyAll is the default if not specified.
     *
     * <p><tt>arg1=val1</tt> is an example of specifying an exact value for a variant property.
     *
     * <p><tt>+arg2</tt> is an explicit copy of the variant property 'arg2' from the context. Does
     * only make sense when not using copyAll, thus when !* is in the link.
     *
     * <p><<tt>-arg3</tt> is a removal (exclusion) of a variant property copied by using copyAll.
     *
     * <p>The arguments will always be specified in alphabetical order, ignoring the + or - symbol.
     * "Not copyAll" (!*) is always the first argument.
     */
    @Override
    public String toString() {
        if (masterRecordId == null && variantProps == null) {
            if (copyAll) {
                // link to self
                return ".";
            } else {
                // link to my master
                return ".!*";
            }
        }

        StringBuilder builder = new StringBuilder();

        if (masterRecordId != null) {
            builder.append(masterRecordId.toString());
        }

        if (!copyAll || variantProps != null) {
            builder.append(".");
            argstoString(builder);
        }

        return builder.toString();
    }

    private void argstoString(StringBuilder builder) {
        boolean firstArg = true;

        if (!copyAll) {
            builder.append("!*");
            firstArg = false;
        }

        if (variantProps != null) {
            for (Map.Entry<String, PropertyValue> entry : variantProps.entrySet()) {
                if (firstArg) {
                    firstArg = false;
                } else {
                    builder.append(";");
                }

                switch (entry.getValue().mode) {
                    case COPY:
                        builder.append('+').append(entry.getKey());
                        break;
                    case REMOVE:
                        builder.append('-').append(entry.getKey());
                        break;
                    case SET:
                        builder.append(entry.getKey()).append('=').append(entry.getValue().value);
                        break;
                }
            }
        }
    }

    public byte[] toBytes() {
        // The bytes format is as follows:
        // [byte representation of master record id, if not null][args: bytes of the string representation][length of args as a short]

        byte[] recordIdBytes = masterRecordId == null ? new byte[0] : masterRecordId.toBytes();

        StringBuilder argsBuilder = new StringBuilder();
        argstoString(argsBuilder);
        byte[] argsBytes;
        try {
            argsBytes = argsBuilder.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }


        if (argsBytes.length > Short.MAX_VALUE) {
            throw new RuntimeException("Unexpected: unreasonably long link arguments: size does not fit in a short: " +
                    argsBytes.length);
        }
        short argsLength = (short)argsBytes.length;


        // Construct the resulting bytes
        byte[] result = new byte[recordIdBytes.length + argsLength + 2];
        int pos = 0;

        // Put in the record id (could be zero-length)
        System.arraycopy(recordIdBytes, 0, result, pos, recordIdBytes.length);
        pos += recordIdBytes.length;

        // Put in the args (could be zero-length)
        System.arraycopy(argsBytes, 0, result, pos, argsBytes.length);
        pos += argsBytes.length;

        // Put in the length of the args
        byte[] argsLengthBytes = new byte[2];
        argsLengthBytes[1] = (byte) argsLength;
        argsLength >>= 8;
        argsLengthBytes[0] = (byte) argsLength;

        System.arraycopy(argsLengthBytes, 0, result, pos, argsLengthBytes.length);

        return result;
    }

    public static Link fromBytes(byte[] bytes, IdGenerator idGenerator) {
        // Format: see toBytes.

        // Read length of the args
        short argsLength = 0;
        argsLength ^= bytes[bytes.length - 2] & 0xFF;
        argsLength <<= 8;
        argsLength ^= bytes[bytes.length - 1] & 0xFF;

        int recordIdLength = bytes.length - 2 - argsLength;

        if (recordIdLength == 0 && argsLength == 0) {
            return new Link();
        }

        LinkBuilder builder = Link.newBuilder();

        if (recordIdLength > 0) {
            byte[] recordIdBytes = new byte[recordIdLength];
            System.arraycopy(bytes, 0, recordIdBytes, 0, recordIdLength);
            RecordId id = idGenerator.fromBytes(recordIdBytes);
            builder.recordId(id);
        }

        if (argsLength > 0) {
            String args = new String(bytes, recordIdLength, argsLength);
            argsFromString(args, builder, args /* does not matter, should never be invalid */);
        }

        return builder.create();
    }

    /**
     * A shortcut for resolve(contextRecord.getId(), idGenerator).
     */
    public RecordId resolve(Record contextRecord, IdGenerator idGenerator) {
        return resolve(contextRecord.getId(), idGenerator);
    }

    /**
     * Resolves this link to a concrete, absolute RecordId.
     *
     * @param contextRecordId usually the id of the record in which this link occurs.
     */
    public RecordId resolve(RecordId contextRecordId, IdGenerator idGenerator) {
        RecordId masterRecordId = this.masterRecordId == null ? contextRecordId.getMaster() : this.masterRecordId;

        Map<String, String> varProps = null;

        // the if statement is just an optimisation to avoid the map creation if not necessary
        if (variantProps != null ||
                (this.masterRecordId != null && !this.masterRecordId.isMaster()) ||
                (copyAll && !contextRecordId.isMaster())) {

            varProps = new HashMap<String, String>();

            // Optionally copy over the properties from the context record id
            if (copyAll) {
                for (Map.Entry<String, String> entry : contextRecordId.getVariantProperties().entrySet()) {
                    if (!varProps.containsKey(entry.getKey())) {
                        varProps.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            // Process the manual specified variant properties
            if (variantProps != null) {
                evalProps(varProps, contextRecordId);
            }
        }

        if (varProps == null || varProps.isEmpty()) {
            return masterRecordId;
        } else {
            return idGenerator.newRecordId(masterRecordId, varProps);
        }
    }

    public enum PropertyMode {SET, COPY, REMOVE}

    public static class PropertyValue {
        private PropertyMode mode;
        private String value;

        private PropertyValue(PropertyMode mode, String value) {
            ArgumentValidator.notNull(mode, "mode");
            this.mode = mode;
            if (mode == PropertyMode.SET) {
                ArgumentValidator.notNull(value, "value");
                this.value = value;
            }
        }

        public PropertyMode getMode() {
            return mode;
        }

        /**
         * Value is only defined when the PropertyMode is SET.
         */
        public String getValue() {
            return value;
        }
    }

    private void evalProps(Map<String, String> resolvedProps, RecordId contextRecordId) {
        Map<String, String> contextProps = contextRecordId.getVariantProperties();

        for (Map.Entry<String, PropertyValue> entry : variantProps.entrySet()) {
            PropertyValue propValue = entry.getValue();
            switch (propValue.mode) {
                case SET:
                    resolvedProps.put(entry.getKey(), propValue.value);
                    break;
                case REMOVE:
                    resolvedProps.remove(entry.getKey());
                    break;
                case COPY:
                    String value = contextProps.get(entry.getKey());
                    if (value != null)
                        resolvedProps.put(entry.getKey(), value);
                    break;
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        Link other = (Link)obj;
        ObjectUtils.safeEquals(masterRecordId, other.masterRecordId);

        if (copyAll != other.copyAll)
            return false;

        if (variantProps == null && other.variantProps == null)
            return true;

        if ((variantProps == null && other.variantProps != null) || (variantProps != null && other.variantProps == null))
            return false;

        if (variantProps.size() != other.variantProps.size())
            return false;

        for (Map.Entry<String, PropertyValue> entry : variantProps.entrySet()) {
            PropertyValue otherVal = other.variantProps.get(entry.getKey());
            if (otherVal == null)
                return false;
            PropertyValue val = entry.getValue();
            if (val.mode != otherVal.mode)
                    return false;
            if (!ObjectUtils.safeEquals(val.value, otherVal.value))
                return false;
        }

        return true;
    }

    public static LinkBuilder newBuilder() {
        return new LinkBuilder();
    }

    public static class LinkBuilder {
        private RecordId masterRecordId;
        private boolean copyAll = true;
        private Map<String, PropertyValue> variantProps;

        private LinkBuilder() {

        }

        /**
         * Calling this resets the state of the variant properties recorded so far.
         */
        public LinkBuilder recordId(RecordId recordId) {
            if (recordId != null) {
                this.masterRecordId = recordId.getMaster();
                this.variantProps = createVariantProps(recordId);
            } else {
                this.masterRecordId = null;
                this.variantProps = null;
            }
            return this;
        }

        public LinkBuilder copyAll(boolean copyAll) {
            this.copyAll = copyAll;
            return this;
        }

        public LinkBuilder copy(String propName) {
            ArgumentValidator.notNull(propName, "propName");
            initVarProps();
            variantProps.put(propName, new PropertyValue(PropertyMode.COPY, null));
            return this;
        }

        public LinkBuilder remove(String propName) {
            ArgumentValidator.notNull(propName, "propName");
            initVarProps();
            variantProps.put(propName, new PropertyValue(PropertyMode.REMOVE, null));
            return this;
        }

        public LinkBuilder set(String propName, String propValue) {
            ArgumentValidator.notNull(propName, "propName");
            ArgumentValidator.notNull(propValue, "propValue");
            initVarProps();
            variantProps.put(propName, new PropertyValue(PropertyMode.SET, propValue));
            return this;
        }

        public Link create() {
            if (variantProps == null) {
                return new Link(masterRecordId, copyAll);
            } else {
                return new Link(masterRecordId, copyAll, new TreeMap<String, PropertyValue>(variantProps));
            }
        }

        private void initVarProps() {
            if (variantProps == null)
                variantProps = new HashMap<String, PropertyValue>();
        }
    }
}
