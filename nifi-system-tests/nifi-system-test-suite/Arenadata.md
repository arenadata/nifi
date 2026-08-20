<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Run following commands from root dir.

### Build nifi project

```bash
./mvnw clean install -DskipTests=true
```

### Build nifi docker image

```bash
./mvnw install -pl nifi-docker/dockermaven -P docker
```

### Start Arenadata integration tests

```bash
mvn install -pl nifi-system-tests/nifi-system-test-suite -P arenadata,adb6
mvn install -pl nifi-system-tests/nifi-system-test-suite -P arenadata,adb7
```
Without specifying adb6/adb7 profile tests will be executed on adb6 by default.