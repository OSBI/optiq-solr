package net.hydromatic.optiq.impl.solr;

import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import org.eigenbase.reltype.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: bugg
 * Date: 16/09/13
 * Time: 21:04
 * To change this template use File | Settings | File Templates.
 */
enum SolrFieldType {
    STRING(null, String.class),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),
    DATE(null, java.util.Date.class),
    TIME(null, java.sql.Time.class),
    TIMESTAMP(null, java.sql.Timestamp.class);

    private final Primitive primitive;
    private final Class clazz;

    private static final Map<String, SolrFieldType> MAP =
            new HashMap<String, SolrFieldType>();

    static {
        for (SolrFieldType value : values()) {
            MAP.put(value.clazz.getSimpleName().toUpperCase(), value);
            if (value.primitive != null) {
                MAP.put(value.primitive.primitiveClass.getSimpleName().toUpperCase(), value);
            }
        }
    }

    SolrFieldType(Primitive primitive) {
        this(primitive, primitive.boxClass);
    }

    SolrFieldType(Primitive primitive, Class clazz) {
        this.primitive = primitive;
        this.clazz = clazz;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        return typeFactory.createJavaType(clazz);
    }

    public static SolrFieldType of(String typeString) {
        return MAP.get(typeString);
    }
}
