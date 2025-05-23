package com.acutus.atk.db.processor;

import com.acutus.atk.db.annotations.FieldFilter;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.annotations.Index;
import com.acutus.atk.db.annotations.Sequence;
import com.acutus.atk.entity.processor.Atk;
import com.acutus.atk.entity.processor.AtkProcessor;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.collection.Tuple4;
import com.sun.source.tree.MethodTree;
import com.sun.source.util.Trees;
import lombok.SneakyThrows;

import javax.annotation.processing.Processor;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeMirror;
import javax.persistence.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.processor.AtkEntity.ColumnNamingStrategy.CAMEL_CASE_UNDERSCORE;
import static com.acutus.atk.db.processor.ProcessorHelper.*;
import static com.acutus.atk.util.AtkUtil.handle;
import static com.acutus.atk.util.StringUtils.*;

@SupportedAnnotationTypes("com.acutus.atk.db.processor.AtkEntity")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
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

    public static String convertCamelCaseToUnderscore(String name) {
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
        line = line.substring(line.lastIndexOf(")") + 1);
        return header + line;
    }

    private static Strings removeColumnAnnotation(Strings lines) {
        lines.firstIndexesOfContains("@javax.persistence.Column")
                .ifPresent(l -> lines.set(l, removeColumnAnnotation(lines.get(l))));
        return lines;
    }

    private static String copyColumn(String colName, Column column) {
        return String.format("name = \"%s\", unique = %s, nullable = %s, insertable = %s, updatable = %s, columnDefinition = \"%s\"" +
                        ", table = \"%s\", length = %d, precision = %d, scale = %d"
                , column.name().isEmpty() ? colName : column.name()
                , column.unique() + "", column.nullable() + "", column.insertable() + "", column.updatable() + "", column.columnDefinition()
                , column.table(), column.length(), column.precision(), column.scale());
    }

    public static class Test {
        @Column(nullable = false, columnDefinition = "varchar(50) default 'AVAILABLE'")
        private String status;

    }

    private String copyTable(String tableName, Table table) {
        return String.format("name = \"%s\""
                , table.name().isEmpty() ? tableName : table.name());
    }

    @Override
    protected String getClassNameLine(Element element, String... removeStrings) {

        String className = super.getClassNameLine(element
                , "@Table", "@javax.persistence.Table", "@com.acutus.atk.db.processor.AtkEntity");

        Table table = element.getAnnotation(Table.class);
        AtkEntity atkEntity = element.getAnnotation(AtkEntity.class);
        String tableName = convertCamelCaseToUnderscore(element.getSimpleName().toString());
        String tableAno =
                atkEntity.type() == AtkEntity.Type.TABLE ?
                        String.format("@Table(%s)", table != null
                                ? copyTable(tableName, table)
                                : String.format("name=\"%s\"", tableName)) :
                        "";

        String interFaces = getInterfaces(element).removeEmpty().toString(",");

        if (!interFaces.isEmpty()) {
            interFaces = "implements  " + interFaces;
        }

        return className.substring(0, className.indexOf("public class "))
                + tableAno + " " + String.format("public class %s extends AbstractAtkEntity<%s,%s> %s{"
                , getClassName(element), getClassName(element), element.getSimpleName(), interFaces);

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
                .filter(c -> !fNames.contains(c) && !isAuditField(c))
                .collect(Collectors.toCollection(Strings::new));
        if (!mismatch.isEmpty()) {
            error(String.format("Index [%s] column mismatch [%s]", index.name(), mismatch));
        }
        return String.format("@Getter\n" +
                        "private transient AtkEnIndex %s = new AtkEnIndex(\"%s\",this);"
                , formatIndexName(index.name()), field.getSimpleName());
    }

    private String getAuditAtkEnField(Element element, String name, String type) {
        return String.format(
                "@Getter\n" +
                        "private transient AtkEnField<%s,%s> _%s = new AtkEnField<>(Reflect.getFields(%s.class).getByName(\"%s\").get(),this)",
                type, getClassName(element), name, getClassName(element), name);
    }

    private boolean isAuditField(String name) {
        return "createdBy".equalsIgnoreCase(name) || "createdDate".equalsIgnoreCase(name)
                || "lastModifiedBy".equalsIgnoreCase(name) || "lastModifiedDate".equalsIgnoreCase(name);

    }

    private Strings getAuditFields(Element element) {
        Strings append = new Strings();
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        if (atk.audit()) {
            append.add("@CreatedBy @Getter @Setter @Column(name = \"created_by\") private String createdBy");
            append.add("@CreatedBy " + getAuditAtkEnField(element, "createdBy", "String"));
            append.add("@CreatedDate @Getter @Column(name = \"created_date\") private LocalDateTime createdDate");
            append.add("@CreatedDate " + getAuditAtkEnField(element, "createdDate", "LocalDateTime"));
            append.add("@LastModifiedBy @Getter @Column(name = \"last_modified_by\") private String lastModifiedBy");
            append.add("@LastModifiedBy " + getAuditAtkEnField(element, "lastModifiedBy", "String"));
            append.add("@LastModifiedDate @Getter @Column(name = \"last_modified_date\") private LocalDateTime lastModifiedDate");
            append.add("@LastModifiedDate " + getAuditAtkEnField(element, "lastModifiedDate", "LocalDateTime"));
        }
        return append;
    }

    private Strings getIndexes(Element element) {
        // add all indexes
        Strings indexes = new Strings();
        Strings fNames = getFieldNames(element, false);
        getFields(element)
                .filter(f -> f.getAnnotation(Index.class) != null)
                .forEach(f -> indexes.add(getIndex(f, f.getAnnotation(Index.class), fNames)));
        return indexes;
    }

    @Override
    protected String getSetterExtra(Element parent, Element e) {
        ForeignKey foreignKey = e.getAnnotation(ForeignKey.class);
        if (foreignKey != null) {
            String key = foreignKey.toString();
            key = key.substring(key.indexOf("table=") + "table=".length());
            key = key.substring(0, key.indexOf(".class"));
            for (Element element : parent.getEnclosedElements()) {
                if (element.asType().toString().equals(key)) {
                    return String.format("\nif (this.get%s() == null || !this.get%s().equals(%s)) this." + element.getSimpleName() + " = null;\n",
                            methodName(e.getSimpleName().toString()), methodName(e.getSimpleName().toString()),
                            e.getSimpleName().toString());
                }
            }
        }
        return "";
    }

    @Override
    protected Strings getStaticFields(Element parent) {
        Strings fields = super.getStaticFields(parent);
        AtkEntity atk = parent.getAnnotation(AtkEntity.class);
        if (atk.audit()) {
            fields.add(getStaticField(parent, "createdBy"));
            fields.add(getStaticField(parent, "createdDate"));
            fields.add(getStaticField(parent, "lastModifiedBy"));
            fields.add(getStaticField(parent, "lastModifiedDate"));
        }
        Sequence sequence = parent.getAnnotation(Sequence.class);
        if (sequence != null) {
            for (String name : sequence.name()) {
                fields.add("\tpublic static final String SEQ_" + name.toUpperCase(Locale.ROOT) + " = \"" + name + "\"");
            }
        }
        return fields.stream().distinct().collect(Collectors.toCollection(Strings::new));
    }

    @Override
    protected Strings getExtraFields(Element element) {
        return getIndexes(element).plus(getAuditFields(element)).prepend("\t");
    }

    @Override
    protected boolean shouldExcludeField(Element element, String name) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        return atk.audit() && isAuditField(name);
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


    @SneakyThrows
    protected String getExecute(Execute execute, Element element) {
        return "\n\n" + getExecuteMethod(element.getSimpleName().toString(), execute);
    }

    protected String getQuery(AtkEntity atk, com.acutus.atk.db.processor.Query query, Element element) {
        TypeMirror returnType = ((ExecutableElement) element).getReturnType();

        String queryMethod = returnType.toString().startsWith("java.util.List")

                ? getQueryAllMethod(query.fetchType(), atk.classNameExt(), returnType, element.getSimpleName().toString(), query.value())
                : getQueryMethod(atk.classNameExt(), returnType, element.getSimpleName().toString(), query.value());

        return "\n\n" + queryMethod;
    }

    protected Strings getExecuteOrQueries(AtkEntity atk, Element element) {
        return element.getEnclosedElements().stream()
                .filter(f -> ElementKind.METHOD.equals(f.getKind()))
                .filter(f -> f.getAnnotation(Execute.class) != null ||
                        f.getAnnotation(com.acutus.atk.db.processor.Query.class) != null)
                .map(f ->
                        f.getAnnotation(Execute.class) != null ?
                                getExecute(((Element) f).getAnnotation(Execute.class), f)
                                : getQuery(atk, ((Element) f).getAnnotation(com.acutus.atk.db.processor.Query.class), f)
                )
                .collect(Collectors.toCollection(Strings::new));
    }

    private String getLazyLoadMethod(String className, Element element, String cType) {
        String eName = element.toString();
        String mName = element.toString();
        mName = "get" + mName.substring(0, 1).toUpperCase() + mName.substring(1);
        return String.format("public AtkEntities<%s> %s(%s c) {\n" +
                "\t%s = %s == null ? %sRef.getAll(c) : %s;\n" +
                "\treturn %s;\n" +
                "}", className, mName, cType, eName, eName, eName, eName, eName);
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
        String className = type.substring(type.indexOf("<") + 1, type.indexOf(">"));
        String classNameAndRef = className + atk.classNameExt();
        String atkRef = String.format("\t@Getter\n" +
                        "private transient AtkEnRelation<%s> %sRef = new AtkEnRelation<>(%s.class, AtkEnRelation.RelType.OneToMany, null, Optional.empty(), this);"
                , classNameAndRef, element, classNameAndRef);

        // TODO add a getter, that wil automatically execute the atkReference

        Strings list = new Strings();
        list.add(element.getAnnotationMirrors().toString());
        list.add(String.format("private transient AtkEntities<%s> %s;", classNameAndRef, element.toString()));
        list.add(getLazyLoadMethod(classNameAndRef, element, "Connection"));
        list.add(getLazyLoadMethod(classNameAndRef, element, "DataSource"));
        return atkRef + "\n\t" + list.toString("\n\t");
    }

    private String getLazyLoadMethodForOptional(String className, Element element, String cType) {
        String eName = element.toString();
        String mName = element.toString();
        mName = "get" + mName.substring(0, 1).toUpperCase() + mName.substring(1);
        return String.format("\tpublic Optional<%s> %s(%s c) {\n" +
                "\t\t%s = %s == null ? %sRef.get(c) : %s;\n" +
                "\t\treturn %s;\n" +
                "\t};", className, mName, cType, eName, eName, eName, eName, eName);
    }


    private String getTargetClass(String oneToRel) {
        oneToRel = oneToRel.substring(oneToRel.indexOf("targetEntity=") + "targetEntity=".length());
        oneToRel = oneToRel.substring(0, oneToRel.length() - 1);
        return "void.class".equals(oneToRel) ? "Optional.empty()" : "Optional.of(" + oneToRel + ")";
    }

    protected Strings getManyToOneImp(AtkEntity atk, Element element) {
        Strings values = new Strings();
        String type = element.asType().toString();
        // check that type is List
        String className = type + atk.classNameExt();

        // add reference
        ManyToOne manyToOne = element.getAnnotation(ManyToOne.class);
        OneToOne oneToOne = element.getAnnotation(OneToOne.class);
        String targetClass = manyToOne != null
                ? getTargetClass(manyToOne.toString()) : oneToOne != null
                ? getTargetClass(oneToOne.toString()) : "Optional.empty()";

        FieldFilter filter = element.getAnnotation(FieldFilter.class);
        String filterStr = filter != null ? "\"" + filter.fields()[0] + "\"" : "null";
        values.add(String.format("\t@Getter\n" +
                        "private transient AtkEnRelation<%s> %sRef = new AtkEnRelation<>(%s.class, AtkEnRelation.RelType." + (manyToOne != null ? "ManyToOne" : "OneToOne") + ",%s,%s, this);"
                , className, element, className, filterStr, targetClass));

        // TODO - Validate that there is exactly one ForeignKey Match
        if (manyToOne != null || oneToOne != null) {
            String fetchType = manyToOne != null ? manyToOne.fetch().name() : oneToOne.fetch().name();
            values.add(String.format("@" + (manyToOne != null ? "ManyToOne" : "OneToOne") + String.format("(fetch = javax.persistence.FetchType.%s)", fetchType)));
            values.add(String.format("private transient Optional<%s> %s;", className, element.toString()));
            values.add(getLazyLoadMethodForOptional(className, element, "Connection"));
            values.add(getLazyLoadMethodForOptional(className, element, "DataSource"));

        } else {
            String mName = "get" + element.toString().substring(0, 1).toUpperCase() + element.toString().substring(1);
            values.add(String.format("\tpublic Optional<%s> %s(DataSource ds) {return %sRef.get(ds);}", className, mName, element.toString()));
            values.add(String.format("\tpublic Optional<%s> %s(Connection c) {return %sRef.get(c);}", className, mName, element.toString()));
        }

        return values;
    }


    protected Strings getOneToMany(AtkEntity atk, Element element) {
        return element.getEnclosedElements().stream()
                .filter(f -> ElementKind.FIELD.equals(f.getKind()) && f.getAnnotation(OneToMany.class) != null)
                .map(f -> getOneToManyImp(atk, f))
                .collect(Collectors.toCollection(Strings::new));
    }

    protected Strings getManyToOne(AtkEntity atk, Element element) {
        return element.getEnclosedElements().stream()
                .filter(f -> ElementKind.FIELD.equals(f.getKind()) &&
                        (f.getAnnotation(ManyToOne.class) != null || f.getAnnotation(OneToOne.class) != null))
                .flatMap(f -> getManyToOneImp(atk, f).stream())
                .collect(Collectors.toCollection(Strings::new));
    }

    @SneakyThrows
    @Override
    protected List<Tuple4<Element, Atk.Match, Boolean, String[]>> getDaoClass(Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        return atk == null || extractDaoClassNames(atk.toString()).isEmpty()
                ? List.of()
                : extractDaoClassNames(atk.toString()).stream()
                .map(c -> new Tuple4<>(getClassElement(c), atk.daoMatch(), atk.daoCopyAll(), atk.daoIgnore())).collect(Collectors.toList());
    }

    @Override
    protected Strings getMethods(String className, Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        // add all query shortcuts
        Strings methods = new Strings();
        methods.add(String.format("\n\tpublic com.acutus.atk.db.Query<%s,%s> query() {return new com.acutus.atk.db.Query(this);}", getClassName(element), element.getSimpleName()));
        methods.add(String.format("\n\tpublic com.acutus.atk.db.Persist<%s> persist() {return new com.acutus.atk.db.Persist(this);}", getClassName(element)));
        methods.add(String.format("\n\tpublic int version() {return %d;}", atk.version()));
        methods.add(String.format("\n\t@Override\n\tpublic AtkEntity.Type getEntityType() {return AtkEntity.Type.%s;}", atk.type().name()));


        methods.add(String.format("\n\t@Override\n\tpublic boolean maintainEntity() {return %s;}", atk.maintainEntity() + ""));
        methods.add(String.format("\n\t@Override\n\tpublic String charset() {return %s;}", "\"" + atk.charset() + "\""));
        methods.add(String.format("\n\t@Override\n\tpublic boolean maintainColumns() {return %s;}", atk.maintainColumns() + ""));
        methods.add(String.format("\n\t@Override\n\tpublic boolean maintainForeignKeys() {return %s;}", atk.maintainForeignKeys() + ""));
        methods.add(String.format("\n\t@Override\n\tpublic boolean maintainIndex() {return %s;}", atk.maintainIndex() + ""));

        // views
        if (atk.type() == AtkEntity.Type.VIEW) {
            methods.add(String.format("\tpublic static String getViewResource() {return getCachedResource(\"%s\");}", atk.viewSqlResource()));
            if (isNotEmpty(atk.viewSqlResource())) {
                methods.add(String.format("\tpublic List<%s> view(Connection c,Object ... params) {return new Query(this).getAllFromResource(c,\"%s\",params);}", getClassName(element), atk.viewSqlResource()));
                methods.add(String.format("\tpublic List<%s> viewFrom(Connection c,String resourceName,Object ... params) {return new Query(this).getAllFromResource(c,resourceName, params);}", getClassName(element)));
                methods.add(String.format("\tpublic void view(Connection c,CallOne<%s> itr, int limit,Object ... params) {new Query(this).getAllFromResource(c,itr,limit,\"%s\",params);}", getClassName(element), atk.viewSqlResource()));
                methods.add(String.format("\tpublic void view(Connection c,String sql, CallOne<%s> itr, int limit,Object ... params) {new Query(this).getAllFromResource(c,itr,limit,sql,params);}", getClassName(element)));
            }
        }

        // add Execute methods
        methods.addAll(getExecuteOrQueries(atk, element));
        methods.addAll(getOneToMany(atk, element));
        methods.addAll(getManyToOne(atk, element));

        if (atk.copyMethods()) {
            for (Element methodDeclaration : element.getEnclosedElements()) {
                if (methodDeclaration instanceof ExecutableElement) {
                    MethodTree methodTree = Trees.instance(getProcessingEnv()).getTree((ExecutableElement) methodDeclaration);

                    if (methodTree != null &&
                            !methodTree.toString().contains("<init>()") &&
                            !methodTree.toString().contains("@Query") &&
                            !methodTree.toString().contains("@Execute")) {
                        methods.add(methodTree.toString());
                    }
                }
            }
        }

        return methods;
    }

    @Override
    protected Strings getImports(Element element) {
        return super.getImports(element)
                .plus("import com.acutus.atk.db.*;")
                .plus("import com.acutus.atk.db.annotations.*;")
                .plus("import static com.acutus.atk.db.annotations.Default.Type.*;")
                .plus("import static com.acutus.atk.db.sql.SQLHelper.runAndReturn;")
                .plus("import static com.acutus.atk.db.sql.SQLHelper.queryOne;")
                .plus("import static com.acutus.atk.db.sql.SQLHelper.query;")
                .plus("import com.acutus.atk.db.sql.SQLHelper;")
                .plus("import static com.acutus.atk.util.AtkUtil.handle;")
                .plus("import java.sql.PreparedStatement;")
                .plus("import com.acutus.atk.db.annotations.audit.*;")
                .plus("import java.time.LocalDateTime;")
                .plus("import javax.persistence.Column;")
                .plus("import javax.persistence.Table;")
                .plus("import java.util.List;")
                .plus("import java.util.Optional;")
                .plus("import java.util.ArrayList;")
                .plus("import java.sql.Connection;")
                .plus("import javax.persistence.OneToMany;")
                .plus("import javax.persistence.ManyToOne;")
                .plus("import javax.persistence.OneToOne;")
                .plus("import javax.sql.DataSource;")
                .plus("import com.acutus.atk.db.*;")
                .plus("import com.acutus.atk.util.collection.*;")
                .plus("import com.acutus.atk.db.processor.AtkEntity;")
                .plus("import static javax.persistence.FetchType.LAZY;")
                .plus("import static javax.persistence.EnumType.STRING;")
                .plus("import com.acutus.atk.util.call.CallOne;")
                .plus("import static com.acutus.atk.db.annotations.ForeignKey.Action.NoAction;")
                .plus("import static com.acutus.atk.db.annotations.ForeignKey.Action.SetDefault;")
                .plus("import static com.acutus.atk.db.annotations.ForeignKey.Action.SetNull;")
                .plus("import static com.acutus.atk.db.annotations.ForeignKey.Action.Cascade;")
                .plus("import static com.acutus.atk.db.annotations.ForeignKey.Action.Restrict;")
                .removeDuplicates();
    }

    @Override
    public String getHashCode(Element element, Strings entity) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        String value = entity.toString("");

        // add lookups to digest
        Populate populate = element.getAnnotation(Populate.class);
        if (populate != null) {
            File file = new File(new File("").getAbsolutePath() + "/src/main/resources/" + populate.value());
            if (file.exists()) {
                value += Files.readString(file.toPath());
            } else {
                value += UUID.randomUUID().toString();
            }
        }

        md.update(value.getBytes());
        byte[] digest = md.digest();
        return bytesToHex(digest).toUpperCase();

    }

    @Override
    protected void validate(Element element) {
        super.validate(element);
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        if (!(atk.trim().equals(ChronoUnit.FOREVER) || atk.audit())) {
            error(String.format("Element [%s]. Is trim is enabled then addAuditFields must also be added", element.getSimpleName()));
        }
    }
}
