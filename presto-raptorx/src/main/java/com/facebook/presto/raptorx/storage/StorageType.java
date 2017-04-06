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
package com.facebook.presto.raptorx.storage;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

public final class StorageType
{
    public static final StorageType BOOLEAN = new StorageType(BOOLEAN_TYPE_NAME);
    public static final StorageType LONG = new StorageType(BIGINT_TYPE_NAME);
    public static final StorageType DOUBLE = new StorageType(DOUBLE_TYPE_NAME);
    public static final StorageType STRING = new StorageType(STRING_TYPE_NAME);
    public static final StorageType BYTES = new StorageType(BINARY_TYPE_NAME);

    private final String hiveType;

    public static StorageType arrayOf(StorageType elementType)
    {
        return new StorageType(format("%s<%s>", LIST_TYPE_NAME, elementType.getHiveType()));
    }

    public static StorageType mapOf(StorageType keyType, StorageType valueType)
    {
        return new StorageType(format("%s<%s,%s>", MAP_TYPE_NAME, keyType.getHiveType(), valueType.getHiveType()));
    }

    public static StorageType decimal(int precision, int scale)
    {
        return new StorageType(format("%s(%d,%d)", DECIMAL_TYPE_NAME, precision, scale));
    }

    private StorageType(String hiveType)
    {
        this.hiveType = requireNonNull(hiveType, "hiveType is null");
    }

    public String getHiveType()
    {
        return hiveType;
    }
}
