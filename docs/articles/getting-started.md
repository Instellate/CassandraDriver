---
uid: articles.getting-started
title: Getting started
---

# Getting started
This article will go through how you can get started with CassandraDriver.  
If you don't have experience with Cassandra can you read more about it [here](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/introduction/).  
This guide will go through how you can setup [CassandraPool](xref:CassandraDriver.CassandraPool)  
You can also take a look at the samples located in the GitHub repo if you don't want to read an article.

All the data and configuration that is used here is what's used by the unit tests.  
If you want to use them yourself for this example can you acquire the compose [here](https://github.com/Instellate/CassandraDriver/blob/c63d354002577865d961b87613425bc2e51191f1/CassandraDriver.Tests/docker-compose.yaml) and the data [here](https://github.com/Instellate/CassandraDriver/blob/c63d354002577865d961b87613425bc2e51191f1/CassandraDriver.Tests/test_data.cql)

## Prerequisites
- A running database, either ScyllaDB or Cassandra

## Setting up a project
We'll be assuming you know how to create a new console app and dependencies so this will just talk about what the packages are and what they do.

### [CassandraDriver](https://www.nuget.org/packages/CassandraDriver)
[CassandraDriver](https://www.nuget.org/packages/CassandraDriver) is the main package. It has all the tools that is required to use the package properly.  
That includes all the tools to connect to a node, managing connections to a cluster, and what not.

### [CassandraDriver.Generators](https://www.nuget.org/packages/CassandraDriver.Generators)
[CassandraDriver.Generators](https://www.nuget.org/packages/CassandraDriver.Generators) main purpose is to allow generating boilerplate code for you. Mainly related to serializing and deserializing the low level output you might get from the library.  
There is also plans for it to in the future to generate basic queries for you.


We recommend adding CassandraDriver and CassandraDriver.Generators as dependencies as those are the ones you'll mainly interact with

## Setting up a pool
A pool is synonymous to a cluster. It's used to communicate between nodes in a cluster flawlessly without needing to care about data redirecting.  
The class that takes care of this is the @CassandraDriver.CassandraPool class. This is what you should primarily interact with unless you plan on going low level with the library.  
To construct a @CassandraDriver.CassandraPool you'll need to use @CassandraDriver.CassandraPoolBuilder. This allow's you to easily configure behaviour for the pool.

## Usage

Using the builder is suppose to be easy and self explanatory. Here is a standard configuration on how you can do it:
```csharp
CassandraPool pool = await CassandraPoolBuilder
    .CreateBuilder()
    .AddNode("172.42.0.2")
    .DiscoverOtherNodes()
    .BlockKeyspace("system")
    .SetDefaultKeyspace("csharpdriver")
    .BuildAsync()
```
This will build a pool that discover other nodes through the node at `172.42.0.2` using the defualt port 9402.  
It also blocks all the `system` keyspaces from being registered in memory. 
CassandraDriver saves all keyspaces and it's table in memory for later use when querying to know which node to query on. 
So blocking `system` might save some memory and startup time.

Querying using the pool is also pretty simple.
```csharp
Statement statement = Statement
    .WithQuery("SELECT * FROM person WHERE name = ?")
    .WithParameters(name)
    .Build();

Query query = await pool.QueryAsync(statement);
```
Notice how instead of needing to specify bindings, it does it for you.  
Other libraries require you to configure the whole query before allowing querying, we try to keep it simple.

@CassandraDriver.Result.Query is iteratable. Keep in mind it will throw an exception if the query isn't related to fetching rows.
```csharp
foreach (Row row in query) 
{
    string name = (string)row["name"]!;
    int userId = (int)row["user_id"]!;
    
    Console.WriteLine($"User has name {name} with user id {userId}");
}
```
Feels a bit inconvenient to convert data? We have generators to help you with that!

```csharp
[CqlDeserialize]
public partial class UserModel
{
    [CqlColumnName("name")]
    public required string Username { get; init; }
    [CqlColumnName("user_id")]
    public required int UserId { get; init; }
}

// Strings implicitly gets converted to `Statement`
List<UserModel> users = await pool.QueryAsync<UserModel>("SELECT * FROM person");
foreach (UserModel user in users) 
{
    string name = user.Username;
    int userId = user.UserId;
    
    Console.WriteLine($"User has name {name} with user id {userId}");
}
```
Specifying the @CassandraDriver.Serialization.CqlDeserializeAttribute will trigger a source generator to implement a @CassandraDriver.ICqlDeserializable`1 which will allow you to read the data as classes instead!  
<small>This only shows a very basic example of generators, more articles later will show you how to use them to their fullest</small>

## Further reading
There is not much more to read about right now. But be on the look out for more in the future!