package com.acutus.atk.db.processor;

import com.acutus.atk.db.Query;
import com.acutus.atk.entity.processor.AtkProcessor;
import com.acutus.atk.util.Strings;
import com.google.auto.service.AutoService;

import javax.annotation.processing.Processor;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.IntStream;

@SupportedAnnotationTypes(
        "com.acutus.atk.db.processor.AtkEntity")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class AtkEntityProcessor extends AtkProcessor {

    public static String GET_FIELDS = "\n" +
            "    public AtkEnFieldList getFields() {\n" +
            "        return (AtkEnFieldList) getRefFields().stream()\n" +
            "                .map(f -> (AtkField) handle(() -> ((Field) f).get(this)))\n" +
            "                .collect(Collectors.toCollection(AtkEnFieldList::new));\n" +
            "    }";

    @Override
    protected String getClassName(Element element) {
        AtkEntity atk = element.getAnnotation(AtkEntity.class);
        return atk.className().isEmpty() ? element.getSimpleName() + atk.classNameExt() :
                atk.className();
    }

    @Override
    protected String getClassNameLine(Element element) {
        return String.format("public class %s extends %s " +
                "implements AbstractAtkEntity {", getClassName(element), element.getSimpleName());
    }

    @Override
    protected Strings getImports() {
        return super.getImports().plus("import com.acutus.atk.db.*");
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

    @Override
    protected Strings getMethods(String className, Element element) {
        // add all query shortcuts
        Strings methods = new Strings();
        Arrays.stream(Query.class.getMethods()).forEach(m -> {
            if ("java.util.Optional<T>".equals(m.getGenericReturnType().getTypeName())
                    || "java.util.List<T>".equals(m.getGenericReturnType().getTypeName())) {
                methods.add(cloneQueryMethod(element, m));
            }
        });
        return Strings.asList(GET_FIELDS).plus(methods);
    }
}
