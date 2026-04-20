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

public class MoneyDataType
        implements ColumnDataType {
    private static final int DEFAULT_PRECISION = 19;
    private static final int DEFAULT_SCALE = 2;
    private final String name = "money";
    private final int precision;
    private final int scale;

    public MoneyDataType() {
        this(DEFAULT_SCALE);
    }

    public MoneyDataType(final int scale) {
        this.precision = DEFAULT_PRECISION;
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException("Money scale must be between 0 and " + precision + ", got " + scale);
        }
        this.scale = scale;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public GreengageDataType getType() {
        return GreengageDataType.MONEY;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public String toString() {
        return "MoneyDataType{" +
                "name='" + name + '\'' +
                ", precision=" + precision +
                ", scale=" + scale +
                '}';
    }
}
