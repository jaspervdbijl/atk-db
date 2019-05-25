package com.acutus.atk.db.processor;

import com.acutus.atk.db.Query;
import com.acutus.atk.db.annotations.Index;
import com.acutus.atk.entity.processor.AtkProcessor;
import com.acutus.atk.util.Strings;
import com.google.auto.service.AutoService;
import lombok.SneakyThrows;

import javax.annotation.processing.Processor;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.persistence.Column;
import javax.persistence.Enumerated;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.processor.AtkEntity.ColumnNamingStrategy.CAMEL_CASE_UNDERSCORE;
import static com.acutus.atk.db.processor.ProcessorHelper.getExecuteMethod;
import static com.acutus.atk.util.StringUtils.removeAllASpaces;

@SupportedAnnotationTypes(
        "com.acutus.atk.db.processor.AtkEntity")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class AtkEntityProcessor extends AtkProcessor {

    @Override
    public boolean isPrimitive(Element e) {
        return super.isPrimitive(e) || e.getAnnotation(Enumerated.class) != null;
    }

    @Override
    protected String getClassName(Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        return atk.className().isEmpty() ? element.getSimpleName() + atk.classNameExt() :
                atk.className();
    }

    private static int UPPER_OFFSET = ((int) 'a') - ((int) 'A');

    private static String convertCamelCaseToUnderscore(String name) {
        String under = "";
        int index = 0;
        for (char c : name.toCharArray()) {
            int dec = (int) c;
            if (dec >= 65 && dec <= 90) {
                under += (index > 0 ? "_" : "") + ((char) (((int) c) + UPPER_OFFSET));
            } else {
                under += c;
            }
            index++;
        }
        return under;
    }

    private static String removeColumnAnnotation(String line) {
        String header = line.substring(0, line.indexOf("@javax.persistence.Column"));
        line = line.substring(line.indexOf("@Column") + "@javax.persistence.Column".length());
        line = line.substring(line.indexOf(")") + 1);
        return header + line;
    }

    private static Strings removeColumnAnnotation(Strings lines) {
        lines.firstIndexesOfContains("@javax.persistence.Column")
                .ifPresent(l -> lines.set(l, removeColumnAnnotation(lines.get(l))));
        return lines;
    }

    private String copyColumn(String colName, Column column) {
        return String.format("name = \"%s\", unique = %s, nullable = %s, insertable = %s, updatable = %s, columnDefinition = \"%s\"" +
                        ", table = \"%s\", length = %d, precision = %d, scale = %d"
                , column.name().isEmpty() ? colName : column.name()
                , column.unique() + "", column.nullable() + "", column.insertable() + "", column.updatable() + "", column.columnDefinition()
                , column.table(), column.length(), column.precision(), column.scale());
    }

    public static void main(String[] args) {
        System.out.println(removeColumnAnnotation("@Id @Column(fasdljsakdjl) @ Id"));
    }

    // TODO support other Table features
    private String copyTable(String tableName, Table table) {
        return String.format("name = \"%s\""
                , table.name().isEmpty() ? tableName : table.name());
    }

    @Override
    protected String getClassNameLine(Element element, String... removeStrings) {

        String className = super.getClassNameLine(element
                , "@Table", "@javax.persistence.Table", "@com.acutus.atk.db.processor.AtkEntity");

        Table table = element.getAnnotation(Table.class);
        String tableName = convertCamelCaseToUnderscore(element.getSimpleName().toString());
        String tableAno = String.format("@Table(%s)", table != null
                ? copyTable(tableName, table)
                : String.format("name=\"%s\"", tableName));

        return className.substring(0, className.indexOf("public class "))
                + tableAno + " " + String.format("public class %s extends AbstractAtkEntity<%s,%s> {"
                , getClassName(element), getClassName(element), element.getSimpleName());

    }

    @Override
    protected String getField(Element root, Element element) {
        String value = super.getField(root, element);
        Strings lines = new Strings(Arrays.asList(value.split("\n")));
        OptionalInt fKindex = lines.transform(s -> removeAllASpaces(s)).getInsideIndex("ForeignKey");
        if (fKindex.isPresent()) {
            lines.set(fKindex.getAsInt(), lines.get(fKindex.getAsInt()).replace(".class", "Entity.class"));
        }
        AtkEntity atkEntity = root.getAnnotation(AtkEntity.class);
        if (atkEntity.columnNamingStrategy().equals(CAMEL_CASE_UNDERSCORE)) {
            Column column = element.getAnnotation(Column.class);
            if (column == null || column.name().isEmpty()) {
                // remove the previous column annotation
                lines = removeColumnAnnotation(lines);
                String colName = convertCamelCaseToUnderscore(element.getSimpleName().toString());
                String columnStr = String.format("@Column(%s)", column != null
                        ? copyColumn(colName, column)
                        : String.format("name=\"%s\"", colName));
                lines.add(0, columnStr);
            }
        }
        return lines.toString("\n");
    }

    private String formatIndexName(String idxName) {
        return "idx" + idxName.substring(0, 1).toUpperCase() + idxName.substring(1);
    }

    private String getIndex(Element field, Index index, Strings fNames) {
        // ensure that all the columns are actual indexes
        Strings mismatch = Arrays.stream(index.columns())
                .filter(c -> !fNames.contains(c))
                .collect(Collectors.toCollection(Strings::new));
        if (!mismatch.isEmpty()) {
            error(String.format("Index [%s] column mismatch [%s]", index.name(), mismatch));
        }
        return String.format("private transient AtkEnIndex %s = new AtkEnIndex(\"%s\",this);"
                , formatIndexName(index.name()), field.getSimpleName());
    }


    private Strings getAuditFields(Element element) {
        Strings append = new Strings();
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        if (atk.addAuditFields()) {
            Strings fNames = getFieldNames(element);
            if (!fNames.contains("createdBy")) {
                append.add("@CreatedBy @Column(name = \"created_by\") private String createdBy");
            }
            if (!fNames.contains("createdDate")) {
                append.add("@CreatedDate @Column(name = \"created_date\") private LocalDateTime createdDate;");
            }
            if (!fNames.contains("lastModifiedBy")) {
                append.add("@LastModifiedBy @Column(name = \"last_modified_by\") private String lastModifiedBy;");
            }
            if (!fNames.contains("lastModifiedDate")) {
                append.add("@LastModifiedDate @Column(name = \"last_modified_date\") private LocalDateTime lastModifiedDate;");
            }
        }
        return append;
    }

    private Strings getIndexes(Element element) {
        // add all indexes
        Strings indexes = new Strings();
        Strings fNames = getFieldNames(element);
        getFields(element)
                .filter(f -> f.getAnnotation(Index.class) != null)
                .forEach(f -> indexes.add(getIndex(f, f.getAnnotation(Index.class), fNames)));
        return indexes;
    }

    @Override
    protected Strings getExtraFields(Element element) {
        return getIndexes(element).plus(getAuditFields(element));
    }

    @Override
    protected String getAtkField(Element parent, Element e) {
        return super.getAtkField(parent, e).replace("AtkField<", "AtkEnField<");
    }

    private String cloneQueryMethod(Element e, Method m) {
        return String.format(
                "public %s<%s> %s(%s) {return new Query<%s>(this).%s(%s);}"
                , m.getReturnType().getName(), getClassName(e), m.getName()
                // add params
                , IntStream.range(0, m.getParameterCount())
                        .mapToObj(i -> m.getParameterTypes()[i].getTypeName() + " p" + i)
                        .reduce((s1, s2) -> s1 + "," + s2).get()
                // add type
                , getClassName(e)
                // add getter name
                , m.getName()
                // add parameters
                , IntStream.range(0, m.getParameterCount())
                        .mapToObj(i -> "p" + i).reduce((s1, s2) -> s1 + "," + s2).get()
        );
    }

    private Strings getQueryMethods(Element element) {
        Strings methods = new Strings();
        Arrays.stream(Query.class.getMethods()).forEach(m -> {
            if ("java.util.Optional<T>".equals(m.getGenericReturnType().getTypeName())
                    || "java.util.List<T>".equals(m.getGenericReturnType().getTypeName())) {
                methods.add(cloneQueryMethod(element, m));
            }
        });
        return methods;
    }

    @SneakyThrows
    protected String getExecute(Execute execute, Element element) {
        return "\n\n" + getExecuteMethod(element.getSimpleName().toString(), execute.value());
    }

    protected Strings getExecutes(Element element) {
        return element.getEnclosedElements().stream()
                .filter(f -> ElementKind.METHOD.equals(f.getKind()))
                .filter(f -> f.getAnnotation(Execute.class) != null)
                .map(f -> getExecute(((Element) f).getAnnotation(Execute.class), f))
                .collect(Collectors.toCollection(Strings::new));
    }

    protected String getOneToManyImp(AtkEntity atk, Element element) {
        String type = element.asType().toString();
        // check that type is List
        if (!type.startsWith("java.util.List")) {
            error(String.format("OneToMany field must be a list [%s]", element.toString()));
        }
        // get the generic type
        if (!type.contains("<")) {
            error(String.format("OneToMany Expected a generic type for [%s]", element.toString()));
        }
        String className = type.substring(type.indexOf("<") + 1, type.indexOf(">")) + atk.classNameExt();
        return String.format("public transient AtkEnRelation<%s> %sRef = new AtkEnRelation<>(%s.class, this);"
                , className, element.toString(), className);
    }

    protected Strings getOneToMany(AtkEntity atk, Element element) {
        return element.getEnclosedElements().stream()
                .filter(f -> ElementKind.FIELD.equals(f.getKind()))
                .filter(f -> f.getAnnotation(OneToMany.class) != null)
                .map(f -> getOneToManyImp(atk, f))
                .collect(Collectors.toCollection(Strings::new));

    }

    @Override
    protected Strings getMethods(String className, Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        // add all query shortcuts
        Strings methods = new Strings();
        methods.add(String.format("public Query<%s> query() {return new Query(this);}", getClassName(element)));
        methods.add(String.format("public Persist<%s> persist() {return new Persist(this);}", getClassName(element)));
        methods.add(String.format("public int version() {return %d;}", atk.version()));

        // add Execute methods
        methods.addAll(getExecutes(element));
        methods.addAll(getOneToMany(atk, element));
        return methods;
    }

    @Override
    protected Strings getImports() {
        return super.getImports().plus("import com.acutus.atk.db.*").plus("import com.acutus.atk.db.annotations.*")
                .plus("import static com.acutus.atk.db.sql.SQLHelper.runAndReturn")
                .plus("import static com.acutus.atk.util.AtkUtil.handle")
                .plus("import java.sql.PreparedStatement")
                .plus("import com.acutus.atk.db.annotations.audit.*")
                .plus("import java.time.LocalDateTime")
                .plus("import javax.persistence.Column")
                .plus("import javax.persistence.Table");
    }
}
