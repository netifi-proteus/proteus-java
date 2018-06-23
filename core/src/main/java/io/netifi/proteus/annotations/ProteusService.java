package io.netifi.proteus.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that identifies Proteus service implementations.
 */
@ProteusAnnotation
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ProteusService {

    /**
     * The group name to use when registering this service with Proteus.
     *
     * @return Proteus group name
     */
    String group();

    /**
     * The destination name to use when registering this service with Proteus. If not specified,
     * a globally unique name will be automatically created by Proteus.
     *
     * @return Proteus destination name
     */
    String destination() default "";
}
