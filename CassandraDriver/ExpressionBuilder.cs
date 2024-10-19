using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using CassandraDriver.Results;

namespace CassandraDriver;

internal static class ExpressionBuilder
{
    public static TReturn CreateDelegate<TReturn>(
        Type type,
        Type buildType,
        string addName
    )
    {
        List<Expression> expressions = [];

        Type genericType = buildType.MakeGenericType(type);
        MethodInfo addMethod = genericType.GetMethod(addName)!;
        MethodInfo parseDataTypesMethod =
            typeof(Row).GetMethod(
                nameof(Row.ParseDataTypes),
                BindingFlags.Static | BindingFlags.NonPublic
            )!;

        ParameterExpression colParam =
            Expression.Parameter(typeof(ColumnValue), "column");
        ParameterExpression lengthParam = Expression.Parameter(typeof(int), "length");
        ParameterExpression spanParam =
            Expression.Parameter(typeof(ReadOnlySpan<byte>).MakeByRefType(), "bytes");

        ConstructorInfo constructor = genericType.GetConstructor([typeof(int)])!;
        ParameterExpression collectionVar =
            Expression.Variable(genericType, "collection");
        expressions.Add(Expression.Assign(
                collectionVar,
                Expression.New(constructor, lengthParam)
            )
        );

        ParameterExpression loopVar = Expression.Variable(typeof(int), "i");
        BinaryExpression initAssign =
            Expression.Assign(loopVar, Expression.Constant(0));
        Expression incrLoopVar =
            Expression.Assign(loopVar, Expression.Increment(loopVar));
        LabelTarget breakLabel = Expression.Label("LoopBreak");

        BlockExpression loop = Expression.Block(
            [loopVar],
            initAssign,
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(loopVar, lengthParam),
                    Expression.Block(Expression.Call(
                            collectionVar,
                            addMethod,
                            Expression.Convert(
                                Expression.Call(
                                    null,
                                    parseDataTypesMethod,
                                    colParam,
                                    spanParam
                                ),
                                type
                            )
                        ),
                        incrLoopVar
                    ),
                    Expression.Break(breakLabel)
                ),
                breakLabel
            )
        );

        expressions.Add(loop);
        expressions.Add(collectionVar);

        Expression<TReturn> lambda =
            Expression
                .Lambda<TReturn>(
                    Expression.Block([collectionVar], expressions),
                    colParam,
                    lengthParam,
                    spanParam
                );

        return lambda.Compile();
    }

    public static TReturn CreateMapDelegate<TReturn>(Type keyType,
        Type valueType)
    {
        List<Expression> expressions = [];

        Type genericType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
        MethodInfo addMethod = genericType.GetMethod(nameof(Dictionary<int, int>.Add))!;
        MethodInfo parseDataTypesMethod =
            typeof(Row).GetMethod(
                nameof(Row.ParseDataTypes),
                BindingFlags.Static | BindingFlags.NonPublic
            )!;

        ParameterExpression keyColParam =
            Expression.Parameter(typeof(ColumnValue), "keyColumn");
        ParameterExpression valueColParam =
            Expression.Parameter(typeof(ColumnValue), "valueColumn");
        ParameterExpression lengthParam = Expression.Parameter(typeof(int), "length");
        ParameterExpression spanParam =
            Expression.Parameter(typeof(ReadOnlySpan<byte>).MakeByRefType(), "bytes");

        ConstructorInfo constructor = genericType.GetConstructor([typeof(int)])!;
        ParameterExpression collectionVar =
            Expression.Variable(genericType, "collection");
        expressions.Add(Expression.Assign(
                collectionVar,
                Expression.New(constructor, lengthParam)
            )
        );

        ParameterExpression loopVar = Expression.Variable(typeof(int), "i");
        BinaryExpression initAssign =
            Expression.Assign(loopVar, Expression.Constant(0));
        Expression incrLoopVar =
            Expression.Assign(loopVar, Expression.Increment(loopVar));
        LabelTarget breakLabel = Expression.Label("LoopBreak");

        BlockExpression loop = Expression.Block(
            [loopVar],
            initAssign,
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(loopVar, lengthParam),
                    Expression.Block(Expression.Call(
                            collectionVar,
                            addMethod,
                            Expression.Convert(
                                Expression.Call(
                                    null,
                                    parseDataTypesMethod,
                                    keyColParam,
                                    spanParam
                                ),
                                keyType
                            ),
                            Expression.Convert(
                                Expression.Call(
                                    null,
                                    parseDataTypesMethod,
                                    valueColParam,
                                    spanParam),
                                valueType
                            )
                        ),
                        incrLoopVar
                    ),
                    Expression.Break(breakLabel)
                ),
                breakLabel
            )
        );

        expressions.Add(loop);
        expressions.Add(collectionVar);

        Expression<TReturn> lambda =
            Expression
                .Lambda<TReturn>(
                    Expression.Block([collectionVar], expressions),
                    keyColParam,
                    valueColParam,
                    lengthParam,
                    spanParam
                );

        return lambda.Compile();
    }
}
