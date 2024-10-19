using CassandraDriver.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace CassandraDriver.Tests;

public class GeneratorTests
{
    [Test]
    public void CqlRowDeserializeTest()
    {
        SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText("""
                                                           using CassandraDriver.Serialization;
                                                           using System.Collections.Generic;

                                                           namespace GeneratorTest;

                                                           [CqlDeserialize] 
                                                           public partial class Test2
                                                           {
                                                                public string Testing { get; set; }
                                                           }

                                                           [CqlDeserialize] 
                                                           public partial class Test 
                                                           {
                                                               [CqlColumnName("test")]
                                                               public string Test { get; set; }
                                                               
                                                               public List<Test2> Tester { get; set; }
                                                           }
                                                           """);

        CSharpCompilation compilation = CSharpCompilation.Create("GeneratorTest",
            [syntaxTree],
            Basic.Reference.Assemblies.Net80.References.All,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        CqlRowDeserializeGenerator generator = new();

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            generators: [generator.AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(default,
                trackIncrementalGeneratorSteps: true));

        GeneratorDriverRunResult
            result = driver.RunGenerators(compilation).GetRunResult();

        Assert.That(result.Results.Select(r => r.Exception), Is.All.Null);
    }
}
