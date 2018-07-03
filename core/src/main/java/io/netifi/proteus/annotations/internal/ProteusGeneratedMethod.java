package io.netifi.proteus.annotations.internal;

import io.netifi.proteus.annotations.ProteusAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@ProteusAnnotation
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ProteusGeneratedMethod {

    /**
     * Type of the class returned from the generated method.
     *
     * @return parameterized type of return class
     */
    Class<?> returnTypeClass();
}
