/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.util.repo;

import org.lilycms.repository.api.*;
import org.lilycms.util.Pair;

import java.io.PrintStream;
import java.util.*;

/**
 * Utilities for producing a readable dump of a Record and a RecordType.
 */
public class PrintUtil {
    public static void print(Record record, Repository repository) {
        print(record, repository, System.out);
    }
    
    public static void print(Record record, Repository repository, PrintStream out) {
        TypeManager typeManager = repository.getTypeManager();

        // Group the fields per scope
        Map<Scope, Map<QName, Object>> fieldsByScope = new HashMap<Scope, Map<QName, Object>>();
        Map<QName, Object> undeterminedFields = new TreeMap<QName, Object>(QNAME_COMP);

        for (Scope scope : Scope.values()) {
            fieldsByScope.put(scope, new TreeMap<QName, Object>(QNAME_COMP));
        }

        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            FieldType fieldType = null;
            try {
                fieldType = typeManager.getFieldTypeByName(field.getKey());
            } catch (Throwable t) {
                // field type failed to load, ignore
            }

            if (fieldType != null) {
                fieldsByScope.get(fieldType.getScope()).put(field.getKey(), field.getValue());
            } else {
                undeterminedFields.put(field.getKey(), field.getValue());
            }
        }

        // Compile a list of all the used namespaces, in the order in which they will be used in the printout.
        List<String> namespaces = new ArrayList<String>();
        for (Scope scope : Scope.values()) {
            for (QName name : fieldsByScope.get(scope).keySet()) {
                if (!namespaces.contains(name.getNamespace())) {
                    namespaces.add(name.getNamespace());
                }
            }
        }
        for (QName name : undeterminedFields.keySet()) {
            if (!namespaces.contains(name.getNamespace())) {
                namespaces.add(name.getNamespace());
            }
        }

        // Produce the printout
        out.println("ID = " + record.getId());
        out.println("Version = " + record.getVersion());
        out.println("Namespaces:");
        for (int i = 0; i < namespaces.size(); i++) {
            out.println("  n" + (i + 1) + " = " + namespaces.get(i));
        }

        // We always print out the non-versioned scope, to show its record type
        out.println("Non-versioned scope:");
        printRecordType(record, Scope.NON_VERSIONED, out);
        printFields(fieldsByScope.get(Scope.NON_VERSIONED), namespaces, out);

        if (fieldsByScope.get(Scope.VERSIONED).size() > 0) {
            out.println("Versioned scope:");
            printRecordType(record, Scope.VERSIONED, out);
            printFields(fieldsByScope.get(Scope.VERSIONED), namespaces, out);
        }

        if (fieldsByScope.get(Scope.VERSIONED_MUTABLE).size() > 0) {
            out.println("Versioned-mutable scope:");
            printRecordType(record, Scope.VERSIONED_MUTABLE, out);
            printFields(fieldsByScope.get(Scope.VERSIONED_MUTABLE), namespaces, out);
        }

        if (undeterminedFields.size() > 0) {
            out.println("Fields of which the field type was not found:");
            printFields(undeterminedFields, namespaces, out);
        }

    }

    private static void printFields(Map<QName, Object> fields, List<String> namespaces, PrintStream out) {
        for (Map.Entry<QName, Object> field : fields.entrySet()) {
            int nsNr = namespaces.indexOf(field.getKey().getNamespace()) + 1;
            out.println("  n" + nsNr + ":" + field.getKey().getName() + " = " + field.getValue());
        }
    }

    private static void printRecordType(Record record, Scope scope, PrintStream out) {
        out.println("  Record type = " + record.getRecordTypeId(scope) + ", version " + record.getRecordTypeVersion(scope));
    }

    public static void print(RecordType recordType, Repository repository) {
        print(recordType, repository, System.out);
    }

    public static void print(RecordType recordType, Repository repository, PrintStream out) {

        List<String> failedFieldTypes = new ArrayList<String>();

        Map<Scope, List<Pair<FieldTypeEntry, FieldType>>> fieldTypeEntriesByScope =
                new HashMap<Scope, List<Pair<FieldTypeEntry, FieldType>>>();
        for (Scope scope : Scope.values()) {
            fieldTypeEntriesByScope.put(scope, new ArrayList<Pair<FieldTypeEntry, FieldType>>());
        }

        for (FieldTypeEntry fte : recordType.getFieldTypeEntries()) {
            FieldType fieldType = null;
            try {
                fieldType = repository.getTypeManager().getFieldTypeById(fte.getFieldTypeId());
            } catch (Throwable t) {
                // field type failed to load
            }

            if (fieldType != null) {
                fieldTypeEntriesByScope.get(fieldType.getScope()).add(new Pair<FieldTypeEntry, FieldType>(fte, fieldType));
            } else {
                failedFieldTypes.add(fte.getFieldTypeId());
            }
        }


        int indent = 0;

        println(out, indent, "ID = " + recordType.getId());
        println(out, indent, "Version = " + recordType.getVersion());
        println(out, indent, "Fields:");

        indent += 2;

        if (fieldTypeEntriesByScope.get(Scope.NON_VERSIONED).size() > 0) {
            println(out, indent, "Non-versioned scope:");
            indent += 2;
            printFieldTypes(fieldTypeEntriesByScope.get(Scope.NON_VERSIONED), out, indent);
            indent -= 2;
        }

        if (fieldTypeEntriesByScope.get(Scope.VERSIONED).size() > 0) {
            println(out, indent, "Versioned scope:");
            indent += 2;
            printFieldTypes(fieldTypeEntriesByScope.get(Scope.VERSIONED), out, indent);
            indent -= 2;
        }

        if (fieldTypeEntriesByScope.get(Scope.VERSIONED_MUTABLE).size() > 0) {
            println(out, indent, "Versioned-mutable scope:");
            indent += 2;
            printFieldTypes(fieldTypeEntriesByScope.get(Scope.VERSIONED_MUTABLE), out, indent);
            indent -= 2;
        }

        if (failedFieldTypes.size() > 0) {
            println(out, indent, "Field types that could not be loaded:");
            indent += 2;
            for (String id : failedFieldTypes) {
                println(out, indent, id);
            }
            indent -= 2;
        }
    }

    private static void printFieldTypes(List<Pair<FieldTypeEntry, FieldType>> fieldTypeEntries, PrintStream out,
            int indent) {

        Collections.sort(fieldTypeEntries, FT_COMP);
        for (Pair<FieldTypeEntry, FieldType> pair : fieldTypeEntries) {
            println(out, indent, "Field");
            indent += 2;
            printFieldType(pair, out, indent);
            indent -= 2;
        }
    }

    private static void printFieldType(Pair<FieldTypeEntry, FieldType> pair, PrintStream out, int indent) {
        FieldTypeEntry fieldTypeEntry = pair.getV1();
        FieldType fieldType = pair.getV2();

        println(out, indent, "ID = " + fieldType.getId());
        println(out, indent, "Name = {" + fieldType.getName().getNamespace() + "}" + fieldType.getName().getName());
        println(out, indent, "Mandatory = " + fieldTypeEntry.isMandatory());
        println(out, indent, "Multi-value = " + fieldType.getValueType().isMultiValue());
        println(out, indent, "Hierarchical = " + fieldType.getValueType().isHierarchical());
        println(out, indent, "Primitive type = " + fieldType.getValueType().getPrimitive().getName());
    }

    private static void println(PrintStream out, int indent, String text) {
        StringBuffer buffer = new StringBuffer(indent);
        for (int i = 0; i < indent; i++) {
            buffer.append(" ");
        }

        out.println(buffer + text);
    }

    private static QNameComparator QNAME_COMP = new QNameComparator();

    private static class QNameComparator implements Comparator<QName> {
        public int compare(QName o1, QName o2) {
            int cmp = o1.getNamespace().compareTo(o2.getNamespace());
            return cmp == 0 ? o1.getName().compareTo(o2.getName()) : cmp;
        }
    }

    private static FieldTypeByQNameComparator FT_COMP = new FieldTypeByQNameComparator();

    private static class FieldTypeByQNameComparator implements Comparator<Pair<FieldTypeEntry, FieldType>> {
        public int compare(Pair<FieldTypeEntry, FieldType> o1, Pair<FieldTypeEntry, FieldType> o2) {
            return QNAME_COMP.compare(o1.getV2().getName(), o2.getV2().getName());
        }
    }
}
