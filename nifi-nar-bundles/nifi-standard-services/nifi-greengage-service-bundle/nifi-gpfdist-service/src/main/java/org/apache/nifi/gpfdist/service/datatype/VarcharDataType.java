/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.datatype;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;

import static java.lang.String.format;

public class VarcharDataType
        implements ColumnDataType {
    private final String name;

    public VarcharDataType(int length) {
        this.name = format("varchar(%d)", length);
    }

    public VarcharDataType() {
        this.name = "varchar";
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public GreengageDataType getType() {
        return GreengageDataType.VARCHAR;
    }

    @Override
    public String toString() {
        return "VarcharDataType{" +
                "name='" + name + '\'' +
                '}';
    }
}
