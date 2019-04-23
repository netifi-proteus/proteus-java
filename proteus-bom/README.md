# Proteus Java Bill Of Material Project

A set of compatible versions for all these projects is curated under a BOM ("Bill of Material").

## Using the BOM with Maven
In Maven, you need to import the bom first:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.netifi.proteus</groupId>
            <artifactId>proteus-bom</artifactId>
            <version>1.6.1</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```
Notice we use the `<dependencyManagement>` section and the `import` scope.

Next, add your dependencies to the relevant proteus projects as usual, except without a 
`<version>`:

```xml
<dependencies>
    <dependency>
        <groupId>io.rsocket</groupId>
        <artifactId>rsocket-core</artifactId>
    </dependency>
    <dependency>
        <groupId>io.rsocket</groupId>
        <artifactId>rsocket-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Using the BOM with Gradle
### Gradle 5.0+
Use the `platform` keyword to import the Maven BOM within the `dependencies` block, then add dependencies to
your project without a version number.

```groovy
dependencies {
     // import BOM
     implementation platform('io.netifi.proteus:proteus-bom:Palladium-BUILD-SNAPSHOT')

     // add dependencies without a version number
     implementation 'io.rsocket:rsocket-core'
}
```

### Gradle 4.x and earlier
Gradle versions prior to 5.0 have no core support for Maven BOMs, but you can use Spring's [`gradle-dependency-management` plugin](https://github.com/spring-gradle-plugins/dependency-management-plugin).

First, apply the plugin from Gradle Plugin Portal (check and change the version if a new one has been released):

```groovy
plugins {
    id "io.spring.dependency-management" version "1.0.7.RELEASE"
}
```
Then use it to import the BOM:

```groovy
dependencyManagement {
     imports {
          mavenBom "io.netifi.proteus:proteus-bom:1.6.1"
     }
}
```

Then add a dependency to your project without a version number:

```groovy
dependencies {
     compile 'io.rsocket:rsocket-core'
}
```

_Sponsored by [Netifi, Inc](https://www.netifi.com)_
