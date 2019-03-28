# Netifi Proteus Java

[![Join the chat at https://gitter.im/netifi/general](https://badges.gitter.im/netifi/general.svg)](https://gitter.im/netifi/general) <a href='https://travis-ci.org/netifi-proteus/proteus-java'><img src='https://travis-ci.org/netifi-proteus/proteus-java.svg?branch=master'></a>


## Build from Source
1. Run the following Gradle command to build the project:

        $ ./gradlew clean build
        
## Updating Dependencies
The proteus-java build uses [Dependencies Lock strategy](https://docs.gradle.org/current/userguide/dependency_locking.html)
Which keeps all dependencies versions snapshot in one file. Such a strategy allows avoiding caveats with libs versions and prevent accidental introduction of new dependencies in the project.

In order to update the dependencies versions, it is required to run the project build with 
additional command as in the following sample

    $ ./gradlew clean build --write-locks
            

## Documentation

## Bugs and Feedback

For bugs, questions, and discussions please use the [Github Issues](https://github.com/proteus/proteus-java/issues).

## License
Copyright 2017 [Netifi Inc.](https://www.netifi.com)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
