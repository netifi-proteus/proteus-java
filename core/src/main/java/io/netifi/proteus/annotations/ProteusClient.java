package io.netifi.proteus.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that identifies Proteus client implementations.
 */
@ProteusAnnotation
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ProteusClient {

    /**
     * The group name that the client will be communicating with.
     *
     * @return Proteus group name
     */
    String group();

    /**
     * The destination name that the client will be communicating with. If not specified, requests
     * by this client will be load balanced across all destinations within the specified group.
     *
     * @return Proteus destination name
     */
    String destination() default "";
}
