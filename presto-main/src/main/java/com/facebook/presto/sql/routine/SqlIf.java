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
package com.facebook.presto.sql.routine;

import com.facebook.presto.sql.relational.RowExpression;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class SqlIf
        implements SqlStatement
{
    private final RowExpression condition;
    private final SqlStatement ifTrue;
    private final Optional<SqlStatement> ifFalse;

    public SqlIf(RowExpression condition, SqlStatement ifTrue)
    {
        this(condition, ifTrue, Optional.empty());
    }

    public SqlIf(RowExpression condition, SqlStatement ifTrue, Optional<SqlStatement> ifFalse)
    {
        this.condition = checkNotNull(condition, "condition is null");
        this.ifTrue = checkNotNull(ifTrue, "ifTrue is null");
        this.ifFalse = checkNotNull(ifFalse, "ifFalse is null");
    }

    public RowExpression getCondition()
    {
        return condition;
    }

    public SqlStatement getIfTrue()
    {
        return ifTrue;
    }

    public Optional<SqlStatement> getIfFalse()
    {
        return ifFalse;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitIf(this, context);
    }
}
