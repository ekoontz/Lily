package org.lilycms.indexer.conf;

import org.lilycms.indexer.formatter.Formatter;
import org.lilycms.repository.api.*;
import org.lilycms.util.repo.VersionTag;
import org.lilycms.util.location.LocationAttributes;
import org.lilycms.util.xml.DocumentHelper;
import org.lilycms.util.xml.LocalXPathExpression;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.XMLConstants;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

// TODO: add some validation of the XML?

// Terminology: the word "field" is usually used for a field from a repository record, while
// the term "index field" is usually used for a field in the index, though sometimes these
// are also just called field.
public class IndexerConfBuilder {
    private static LocalXPathExpression INDEX_CASES =
            new LocalXPathExpression("/indexer/cases/case");

    private static LocalXPathExpression FORMATTERS =
            new LocalXPathExpression("/indexer/formatters/formatter");

    private static LocalXPathExpression INDEX_FIELDS =
            new LocalXPathExpression("/indexer/indexFields/indexField");

    private Document doc;

    private IndexerConf conf;

    private Repository repository;
    private TypeManager typeManager;

    private IndexerConfBuilder() {
        // prevents instantiation
    }

    public static IndexerConf build(InputStream is, Repository repository) throws IndexerConfException {
        Document doc;
        try {
            doc = DocumentHelper.parse(is);
        } catch (Exception e) {
            throw new IndexerConfException("Error parsing supplied configuration.", e);
        }
        return new IndexerConfBuilder().build(doc, repository);
    }

    private IndexerConf build(Document doc, Repository repository) throws IndexerConfException {
        validate(doc);
        this.doc = doc;
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.conf = new IndexerConf();

        try {
            buildCases();
            buildFormatters();
            buildIndexFields();
        } catch (Exception e) {
            throw new IndexerConfException("Error in the configuration.", e);
        }

        return conf;
    }

    private void buildCases() throws Exception {
        List<Element> cases = INDEX_CASES.get().evalAsNativeElementList(doc);
        for (Element caseEl : cases) {
            // TODO will need resolving of the QName
            String recordType = DocumentHelper.getAttribute(caseEl, "recordType", true);
            String vtagsSpec = DocumentHelper.getAttribute(caseEl, "vtags", false);
            boolean indexVersionless = DocumentHelper.getBooleanAttribute(caseEl, "indexVersionless", false);

            Map<String, String> varPropsPattern = parseVariantPropertiesPattern(caseEl);
            Set<String> vtags = parseVersionTags(vtagsSpec);

            IndexCase indexCase = new IndexCase(recordType, varPropsPattern, vtags, indexVersionless);
            conf.addIndexCase(indexCase);
        }
    }

    private void buildFormatters() throws Exception {
        List<Element> formatters = FORMATTERS.get().evalAsNativeElementList(doc);
        for (Element formatterEl : formatters) {
            String className = DocumentHelper.getAttribute(formatterEl, "class", true);
            Formatter formatter = instantiateFormatter(className);

            String name = DocumentHelper.getAttribute(formatterEl, "name", false);
            String type = DocumentHelper.getAttribute(formatterEl, "type", false);

            Set<String> types;
            if (type == null) {
                types = formatter.getSupportedPrimitiveValueTypes();
            } else if (type.trim().equals("*")) {
                types = Collections.emptySet();
            } else {
                // Check the specified types are a subset of those supported by the formatter
                types = new HashSet<String>();
                Set<String> supportedTypes = formatter.getSupportedPrimitiveValueTypes();
                List<String> specifiedTypes = parseCSV(type);
                for (String item : specifiedTypes) {
                    if (supportedTypes.contains(item)) {
                        types.add(item);
                    } else {
                        throw new IndexerConfException("Formatter definition error: primitive value type "
                                + item + " is not supported by formatter " + className);
                    }
                }
            }

            boolean singleValue = DocumentHelper.getBooleanAttribute(formatterEl, "singleValue", formatter.supportsSingleValue());
            boolean multiValue = DocumentHelper.getBooleanAttribute(formatterEl, "multiValue", formatter.supportsMultiValue());
            boolean nonHierarchical = DocumentHelper.getBooleanAttribute(formatterEl, "nonHierarchical", formatter.supportsNonHierarchicalValue());
            boolean hierarchical = DocumentHelper.getBooleanAttribute(formatterEl, "hierarchical", formatter.supportsHierarchicalValue());

            String message = "Formatter does not support %1$s. Class " + className + " at " + LocationAttributes.getLocation(formatterEl);

            if (singleValue && !formatter.supportsSingleValue())
                throw new IndexerConfException(String.format(message, "singleValue"));
            if (multiValue && !formatter.supportsMultiValue())
                throw new IndexerConfException(String.format(message, "multiValue"));
            if (hierarchical && !formatter.supportsHierarchicalValue())
                throw new IndexerConfException(String.format(message, "hierarchical"));
            if (nonHierarchical && !formatter.supportsNonHierarchicalValue())
                throw new IndexerConfException(String.format(message, "nonHierarchical"));

            if (name != null && conf.getFormatters().hasFormatter(name)) {
                throw new IndexerConfException("Duplicate formatter name: " + name);
            }

            conf.getFormatters().addFormatter(formatter, name, types, singleValue, multiValue, nonHierarchical, hierarchical);
        }
    }

    private Formatter instantiateFormatter(String className) throws IndexerConfException {
        ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
        Class formatterClass;
        try {
            formatterClass = contextCL.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IndexerConfException("Error loading formatter class " + className + " from context class loader.", e);
        }

        if (!Formatter.class.isAssignableFrom(formatterClass)) {
            throw new IndexerConfException("Specified formatter class does not implement Formatter interface: " + className);
        }

        try {
            return (Formatter)formatterClass.newInstance();
        } catch (Exception e) {
            throw new IndexerConfException("Error instantiating formatter class " + className, e);
        }
    }

    private List<String> parseCSV(String csv) {
        String[] parts = csv.split(",");

        List<String> result = new ArrayList<String>(parts.length);

        for (String part : parts) {
            part = part.trim();
            if (part.length() > 0)
                result.add(part);
        }

        return result;
    }

    private Map<String, String> parseVariantPropertiesPattern(Element caseEl) throws Exception {
        String variant = DocumentHelper.getAttribute(caseEl, "variant", false);

        Map<String, String> varPropsPattern = new HashMap<String, String>();

        if (variant == null)
            return varPropsPattern;

        String[] props = variant.split(",");
        for (String prop : props) {
            prop = prop.trim();
            if (prop.length() > 0) {
                int eqPos = prop.indexOf("=");
                if (eqPos != -1) {
                    String propName = prop.substring(0, eqPos);
                    String propValue = prop.substring(eqPos + 1);
                    if (propName.equals("*")) {
                        throw new IndexerConfException(String.format("Error in variant attribute: the character '*' " +
                                "can only be used as wildcard, not as variant dimension name, attribute = %1$s, at: %2$s",
                                variant, LocationAttributes.getLocation(caseEl)));
                    }
                    varPropsPattern.put(propName, propValue);
                } else {
                    varPropsPattern.put(prop, null);
                }
            }
        }

        return varPropsPattern;
    }

    private Set<String> parseVersionTags(String vtagsSpec) throws IndexerConfException {
        Set<String> vtags = new HashSet<String>();

        if (vtagsSpec == null)
            return vtags;

        String[] tags = vtagsSpec.split(",");
        for (String tag : tags) {
            tag = tag.trim();
            if (tag.length() > 0) {
                try {
                    vtags.add(typeManager.getFieldTypeByName(VersionTag.qname(tag)).getId());
                } catch (FieldTypeNotFoundException e) {
                    throw new IndexerConfException("unknown vtag used in indexer configuration: " + tag);
                } catch (TypeException e) {
                    throw new IndexerConfException("error loading field type for vtag: " + tag, e);
                }
            }
        }

        return vtags;
    }

    private void buildIndexFields() throws Exception {
        List<Element> indexFields = INDEX_FIELDS.get().evalAsNativeElementList(doc);
        for (Element indexFieldEl : indexFields) {
            String name = DocumentHelper.getAttribute(indexFieldEl, "name", true);
            validateName(name);
            Element valueEl = DocumentHelper.getElementChild(indexFieldEl, "value", true);
            Value value = buildValue(valueEl);

            IndexField indexField = new IndexField(name, value);
            conf.addIndexField(indexField);
        }
    }

    private void validateName(String name) throws IndexerConfException {
        if (name.startsWith("@@")) {
            throw new IndexerConfException("names starting with @@ are reserved for internal uses. Name: " + name);
        }
    }

    private Value buildValue(Element valueEl) throws Exception {
        Element fieldEl = DocumentHelper.getElementChild(valueEl, "field", false);
        Element derefEl = DocumentHelper.getElementChild(valueEl, "deref", false);

        FieldType fieldType;
        Value value;

        boolean extractContent = DocumentHelper.getBooleanAttribute(valueEl, "extractContent", false);
        String formatter = DocumentHelper.getAttribute(valueEl, "formatter", false);
        if (formatter != null && !conf.getFormatters().hasFormatter(formatter)) {
            throw new IndexerConfException("Formatter does not exist: " + formatter + " at " +
                    LocationAttributes.getLocationString(valueEl));
        }

        if (fieldEl != null) {
            fieldType = getFieldType(DocumentHelper.getAttribute(fieldEl, "name", true), fieldEl);
            value = new FieldValue(fieldType, extractContent, formatter, conf.getFormatters());
        } else if (derefEl != null) {
            Element[] children = DocumentHelper.getElementChildren(derefEl);

            Element lastEl = children[children.length - 1];

            if (!lastEl.getLocalName().equals("field") || lastEl.getNamespaceURI() != null) {
                throw new IndexerConfException("Last element in a <deref> should be a field, at " +
                        LocationAttributes.getLocationString(lastEl));
            }

            if (children.length == 1) {
                throw new IndexerConfException("A <deref> should contain one or more <follow> elements.");
            }

            fieldType = getFieldType(DocumentHelper.getAttribute(lastEl, "name", true), lastEl);

            DerefValue deref = new DerefValue(fieldType, extractContent, formatter, conf.getFormatters());

            // Run over all children except the last
            for (int i = 0; i < children.length - 1; i++) {
                Element child = children[i];
                if (child.getLocalName().equals("follow") || child.getNamespaceURI() == null) {
                    String field = DocumentHelper.getAttribute(child, "field", false);
                    String variant = DocumentHelper.getAttribute(child, "variant", false);
                    if (field != null) {
                        FieldType followField = getFieldType(DocumentHelper.getAttribute(child, "field", true), child);
                        if (!followField.getValueType().getPrimitive().getName().equals("LINK")) {
                            throw new IndexerConfException("Follow-field is not a link field: " + followField.getName()
                                    + " at " + LocationAttributes.getLocation(child));
                        }
                        if (followField.getValueType().isHierarchical()) {
                            throw new IndexerConfException("Follow-field should not be a hierarchical link field: "
                                    + followField.getName() + " at " + LocationAttributes.getLocation(child));
                        }
                        deref.addFieldFollow(followField);
                    } else if (variant != null) {
                        if (variant.equals("master")) {
                            deref.addMasterFollow();
                        } else {
                            // The variant dimensions are specified in a syntax like "-var1,-var2,-var3"
                            boolean validConfig = true;
                            Set<String> dimensions = new HashSet<String>();
                            String[] ops = variant.split(",");
                            for (String op : ops) {
                                op = op.trim();
                                if (op.length() > 1 && op.startsWith("-")) {
                                    String dimension = op.substring(1);
                                    dimensions.add(dimension);
                                } else {
                                    validConfig = false;
                                    break;
                                }
                            }
                            if (dimensions.size() == 0)
                                validConfig = false;

                            if (!validConfig) {
                                throw new IndexerConfException("Invalid specification of variants to follow: \"" +
                                        variant + "\".");
                            }

                            deref.addVariantFollow(dimensions);
                        }
                    } else {
                        throw new IndexerConfException("Required attribute missing on <follow> at " +
                                LocationAttributes.getLocation(child));
                    }
                } else {
                    throw new IndexerConfException("Unexpected element <" + child.getTagName() + "> at " +
                            LocationAttributes.getLocation(child));
                }
            }

            deref.init(typeManager);
            value = deref;
        } else {
            throw new IndexerConfException("No value configured for index field at "
                    + LocationAttributes.getLocation(valueEl));
        }

        if (extractContent && !fieldType.getValueType().getPrimitive().getName().equals("BLOB")) {
            throw new IndexerConfException("extractContent is used for a non-blob value at "
                    + LocationAttributes.getLocation(valueEl));
        }

        return value;
    }

    private QName parseQName(String qname, Element contextEl) throws IndexerConfException {
        int colonPos = qname.indexOf(":");
        if (colonPos == -1) {
            throw new IndexerConfException("Field name is not a qualified name, it should include a namespace prefix: " + qname);
        }

        String prefix = qname.substring(0, colonPos);
        String localName = qname.substring(colonPos + 1);

        String uri = contextEl.lookupNamespaceURI(prefix);
        if (uri == null) {
            throw new IndexerConfException("Prefix does not resolve to a namespace: " + qname);
        }

        return new QName(uri, localName);
    }

    private String getFieldTypeId(String qname, Element contextEl) throws IndexerConfException {
        QName parsedQName = parseQName(qname, contextEl);

        try {
            return typeManager.getFieldTypeByName(parsedQName).getId();
        } catch (FieldTypeNotFoundException e) {
            throw new IndexerConfException("unknown field type: " + parsedQName, e);
        } catch (TypeException e) {
            throw new IndexerConfException("error loading field type: " + parsedQName, e);
        }
    }

    private FieldType getFieldType(String qname, Element contextEl) throws IndexerConfException {
        QName parsedQName = parseQName(qname, contextEl);

        try {
            return typeManager.getFieldTypeByName(parsedQName);
        } catch (FieldTypeNotFoundException e) {
            throw new IndexerConfException("unknown field type: " + parsedQName, e);
        } catch (TypeException e) {
            throw new IndexerConfException("error loading field type: " + parsedQName, e);
        }
    }

    private void validate(Document document) throws IndexerConfException {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            URL url = getClass().getClassLoader().getResource("org/lilycms/indexer/conf/indexerconf.xsd");
            Schema schema = factory.newSchema(url);
            Validator validator = schema.newValidator();
            validator.validate(new DOMSource(document));
        } catch (Exception e) {
            throw new IndexerConfException("Error validating indexer configuration against XML Schema.", e);
        }
    }
}
