package com.acutus.atk.db.processor;

import com.acutus.atk.db.Query;
import com.acutus.atk.db.annotations.Index;
import com.acutus.atk.entity.processor.AtkProcessor;
import com.acutus.atk.util.Strings;
import com.google.auto.service.AutoService;

import javax.annotation.processing.Processor;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        return String.format("public class %s extends AbstractAtkEntity {", getClassName(element));
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
        return super.getImports().plus("import com.acutus.atk.db.*").plus("import com.acutus.atk.db.annotations.*");
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

    @Override
    protected Strings getMethods(String className, Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        // add all query shortcuts
        Strings methods = new Strings();
        methods.add(String.format("public Query<%s> query() {return new Query(this);}", getClassName(element)));
        methods.add(String.format("public Persist<%s> persist() {return new Persist(this);}", getClassName(element)));
        methods.add(String.format("public int version() {return %d;}", atk.version()));
        return methods;
    }
}
