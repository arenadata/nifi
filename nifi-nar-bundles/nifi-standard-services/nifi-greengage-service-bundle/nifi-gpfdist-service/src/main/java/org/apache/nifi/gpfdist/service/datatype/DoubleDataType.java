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

public class DoubleDataType
        implements ColumnDataType {
    private final String name;

    public DoubleDataType() {
        this.name = "double precision";
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public GreengageDataType getType() {
        return GreengageDataType.DOUBLE_PRECISION;
    }

    @Override
    public String toString() {
        return "DoubleDataType{" +
                "name='" + name + '\'' +
                '}';
    }
}
