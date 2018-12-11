package com.acutus.atk.db.processor;

import com.acutus.atk.entity.processor.AtkProcessor;
import com.acutus.atk.util.Strings;
import com.google.auto.service.AutoService;
import com.sun.org.apache.xalan.internal.xsltc.runtime.InternalRuntimeError;

import javax.annotation.processing.Processor;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.tools.Diagnostic;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

@SupportedAnnotationTypes(
        "com.acutus.atk.db.processor.AtkEntity")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class AtkEntityProcessor extends AtkProcessor  {

    @Override
    protected Strings getElement(String className, Element element) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, "Mk0");

        Strings entity = super.getElement(className,element);

        processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, "Mk1");

        Integer importIndex = entity.firstIndexesOfContains("import ").get();
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Import index " + importIndex);
        Integer extendsIndex = entity
                .firstIndexesOfContains("Imp extends com.acutus.atk.entity.AbstractAtk {").get();
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Extends index " + extendsIndex);

        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Mk2");
        // fix the extension
        entity.set(extendsIndex,entity.get(extendsIndex).replace("entity.AbstractAtk","db.AbstractAtkEntity"));
        entity.add(importIndex,"import com.acutus.atk.db.*;");
        // now add query builder method
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Compile " + entity);
        return entity.replace("AtkField<","AtkEnField<");
    }
}
