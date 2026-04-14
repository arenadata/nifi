package org.apache.nifi.gpfdist.service.datatype;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;

import java.util.Objects;

public class EnumDataType implements ColumnDataType {
    private final String name;
    private final String typeName;
    private final String typeSchema;

    public EnumDataType(final String typeSchema, final String typeName) {
        this.typeSchema = (typeSchema == null || typeSchema.isBlank()) ? null : typeSchema;
        this.typeName = Objects.requireNonNull(typeName, "typeName cannot be null");
        this.name = (this.typeSchema == null) ? this.typeName : (this.typeSchema + "." + this.typeName);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public GreengageDataType getType() {
        return GreengageDataType.ENUM;
    }

    public String getEnumTypeName() {
        return typeName;
    }

    public String getEnumTypeSchema() {
        return typeSchema;
    }

    @Override
    public String toString() {
        return "EnumDataType{" +
                "name='" + name + '\'' +
                ", typeName='" + typeName + '\'' +
                ", typeSchema='" + typeSchema + '\'' +
                '}';
    }
}
