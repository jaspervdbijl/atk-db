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
import javax.persistence.OneToMany;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.processor.ProcessorHelper.getExecuteMethod;
import static com.acutus.atk.util.StringUtils.removeAllASpaces;

@SupportedAnnotationTypes(
        "com.acutus.atk.db.processor.AtkEntity")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class AtkEntityProcessor extends AtkProcessor {

    @Override
    protected String getClassName(Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        return atk.className().isEmpty() ? element.getSimpleName() + atk.classNameExt() :
                atk.className();
    }

    @Override
    protected String getClassNameLine(Element element) {
        String className = super.getClassNameLine(element);
        className = className.replace("@com.acutus.atk.db.processor.AtkEntity", "");
        return className.substring(0, className.indexOf("public class "))
                + String.format("public class %s extends AbstractAtkEntity<%s,%s> {"
                , getClassName(element), getClassName(element), element.getSimpleName());
    }

    @Override
    protected String getField(Element element) {
        String value = super.getField(element);
        Strings lines = new Strings(Arrays.asList(value.split("\n")));
        OptionalInt fKindex = lines.transform(s -> removeAllASpaces(s)).getInsideIndex("ForeignKey");
        if (fKindex.isPresent()) {
            info("Got fKindex + " + fKindex.getAsInt() + "-> " + lines.get(fKindex.getAsInt()));
            lines.set(fKindex.getAsInt(), lines.get(fKindex.getAsInt()).replace(".class", "Entity.class"));
            value = lines.toString("\n");
        }
        return value;
    }

    @Override
    protected Strings getImports() {
        return super.getImports().plus("import com.acutus.atk.db.*").plus("import com.acutus.atk.db.annotations.*")
                .plus("import static com.acutus.atk.db.sql.SQLHelper.runAndReturn")
                .plus("import static com.acutus.atk.util.AtkUtil.handle")
                .plus("import java.sql.PreparedStatement");
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

    private Strings getIndexes(Element element) {
        // add all indexes
        Strings indexes = new Strings();
        Strings fNames = element.getEnclosedElements().stream()
                .filter(f -> ElementKind.FIELD.equals(f.getKind()))
                .map(f -> f.getSimpleName().toString())
                .collect(Collectors.toCollection(Strings::new));
        element.getEnclosedElements().stream()
                .filter(f -> ElementKind.FIELD.equals(f.getKind()))
                .filter(f -> f.getAnnotation(Index.class) != null)
                .forEach(f -> indexes.add(getIndex(f, f.getAnnotation(Index.class), fNames)));
        return indexes;
    }

    @Override
    protected Strings getExtraFields(Element element) {
        return getIndexes(element);
    }

    @Override
    protected String getAtkField(Element parent, Element e) {
        return super.getAtkField(parent, e).replace("AtkField<", "AtkEnField<");
    }

    private String cloneQueryMethod(Element e, Method m) {
        info(m.getName());
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
}
