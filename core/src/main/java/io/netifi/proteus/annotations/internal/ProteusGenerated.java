package io.netifi.proteus.annotations.internal;

import io.netifi.proteus.annotations.ProteusAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that identifies proteus generated services and stores metadata that can
 * be used by dependency injection frameworks and custom annotation processors.
 */
@ProteusAnnotation
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ProteusGenerated {

    /**
     * Type of the generated Proteus resource.
     *
     * @return type of generated resource
     */
    ProteusResourceType type();

    /**
     * Class of the Proteus service hosted by the annotated class.
     * 
     * @return Proteus service class
     */
    Class<?> idlClass();
}
