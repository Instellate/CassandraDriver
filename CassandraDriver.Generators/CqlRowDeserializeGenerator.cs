using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CassandraDriver.Generators;

[Generator]
public class CqlRowDeserializeGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        context.RegisterPostInitializationOutput(postInit =>
        {
            postInit.AddSource("attributes.g.cs",
                """
                using System;

                namespace CassandraDriver.Serialization
                {
                    [AttributeUsage(AttributeTargets.Class)]
                    public class CqlDeserializeAttribute : Attribute
                    {
                    }
                    
                    [AttributeUsage(AttributeTargets.Property)]
                    public class CqlColumnNameAttribute : Attribute
                    {
                        public string ColumnName { get; }
                    
                        public CqlColumnNameAttribute(string columnName) 
                        {
                            this.ColumnName = columnName;
                        }
                    }
                }
                """);
        });

        IncrementalValuesProvider<ClassModel> pipeline
            = context.SyntaxProvider.ForAttributeWithMetadataName(
                "CassandraDriver.Serialization.CqlDeserializeAttribute",
                static (node, _) => node is ClassDeclarationSyntax,
                static (context, _) =>
                {
                    INamedTypeSymbol target
                        = (INamedTypeSymbol)context.TargetSymbol;

                    EquatableList<PropertyModel> properties = [];

                    foreach (IPropertySymbol property in target.GetMembers()
                                 .OfType<IPropertySymbol>())
                    {
                        ITypeSymbol typeSymbol = property.Type;

                        PropertyBehaviour behaviour;
                        if (property.Type is INamedTypeSymbol namedType)
                        {
                            string constructedType
                                = namedType.ConstructedFrom.ToDisplayString();

                            switch (constructedType)
                            {
                                case "System.Collections.Generic.List<T>":
                                    typeSymbol = namedType.TypeArguments[0];
                                    behaviour = PropertyBehaviour.List;
                                    break;
                                case "System.Collections.Generic.Dictionary<TKey, TValue>"
                                    :
                                    behaviour = PropertyBehaviour.Dictionary;
                                    break;
                                default:
                                    behaviour = PropertyBehaviour.Normal;
                                    break;
                            }
                        }
                        else
                        {
                            behaviour = PropertyBehaviour.Normal;
                        }

                        string type = typeSymbol.ToDisplayString(
                            SymbolDisplayFormat.FullyQualifiedFormat);

                        string name = property.Name;

                        bool implementsDeserializable = typeSymbol.GetAttributes().Any(
                            a => a.AttributeClass?.ToDisplayString().Equals(
                                     "CassandraDriver.Serialization.CqlDeserializeAttribute") ??
                                 false
                        );

                        string columnName;
                        AttributeData? columnNameAttr = property.GetAttributes()
                            .FirstOrDefault(a =>
                                a.AttributeClass?.ToDisplayString()
                                    .Equals(
                                        "CassandraDriver.Serialization.CqlColumnNameAttribute") ??
                                false);
                        if (columnNameAttr is not null)
                        {
                            columnName
                                = (string)columnNameAttr.ConstructorArguments[0]
                                    .Value!;
                        }
                        else
                        {
                            columnName = name;
                        }

                        properties.Add(new PropertyModel(name,
                            new PropertyType(type, behaviour, implementsDeserializable),
                            columnName));
                    }

                    return new ClassModel(target.ContainingNamespace?.ToDisplayString(
                            SymbolDisplayFormat.FullyQualifiedFormat
                                .WithGlobalNamespaceStyle(
                                    SymbolDisplayGlobalNamespaceStyle.Omitted))!,
                        target.Name,
                        properties);
                });

        context.RegisterSourceOutput(pipeline,
            static (ctx, classModel) =>
            {
                StringBuilder sb = new();
                sb.Append($$"""
                            using CassandraDriver.Results;
                            using CassandraDriver;

                            namespace {{classModel.Namespace}};
                              
                            public partial class {{classModel.ClassName}} : ICqlDeserializable<{{classModel.ClassName}}>
                            {
                                public static {{classModel.ClassName}} DeserializeRow(Row row)
                                {

                            """);

                List<string> values = [];
                foreach (PropertyModel property in classModel.Properties)
                {
                    switch (property.PropertyType.Behaviour)
                    {
                        case PropertyBehaviour.List:
                            values.Add(CreateList(ref sb, property));
                            break;
                        case PropertyBehaviour.Normal:
                            values.Add(CreateNormal(property));
                            break;
                    }
                }

                sb.Append($$$"""
                                     return new {{{classModel.ClassName}}}() 
                                     {

                             """);

                foreach (string value in values)
                {
                    sb.Append(value);
                }

                sb.Append("""
                                  };
                              }
                          }
                          """);

                string fileName = $"{classModel.ClassName}_ICqlDeserializable.g.cs";
                ctx.AddSource(fileName, sb.ToString());
            });
    }

    private static string CreateList(ref StringBuilder sb, PropertyModel property)
    {
        if (property.PropertyType.ImplementsDeserializable)
        {
            sb.Append(
                $"        List<{property.PropertyType.Type}> {property.PropertyName}List = new(((List<Row>)row[\"{property.ColumnName}\"]).Count);\n");
            sb.Append($$"""
                                foreach (var value in (List<Row>)row["{{property.ColumnName}}"])
                                {
                                    {{property.PropertyName}}List.Add(
                        """);
            sb.Append(
                $"{property.PropertyType.Type}.DeserializeRow((Row)row[\"{property.ColumnName}\"]));\n");
            sb.Append("        }\n");
            return
                $"            {property.PropertyName} = {property.PropertyName}List,\n";
        }
        else
        {
            return
                $"            {property.PropertyName} = (List<{property.PropertyType.Type}>),\n";
        }
    }

    private static string CreateNormal(PropertyModel property)
    {
        StringBuilder sb = new();
        sb.Append($"            {property.PropertyName} = ");
        sb.Append(
            property.PropertyType.ImplementsDeserializable
                ? $"{property.PropertyType.Type}.DeserializeRow((Row)row[\"{property.ColumnName}\"]),\n"
                : $"({property.PropertyType.Type})row[\"{property.ColumnName}\"],\n");

        return sb.ToString();
    }


    private record ClassModel(
        string Namespace,
        string ClassName,
        EquatableList<PropertyModel> Properties);

    private record PropertyModel(
        string PropertyName,
        PropertyType PropertyType,
        string ColumnName);

    private record PropertyType(
        string Type,
        PropertyBehaviour Behaviour,
        bool ImplementsDeserializable);

    private enum PropertyBehaviour
    {
        Normal,
        List,
        Dictionary
    }
}
