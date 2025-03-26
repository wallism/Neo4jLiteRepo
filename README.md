# Neo4jLiteRepo

The goal of this repo is to simplify use of Neo4j for .Net developers.

By following a few basic design design patterns, the barrier to getting your data into Neo4j should be simplified.

The primary project that you would re-use is Neo4jLiteRepo. The other project (.Importer and .Sample) exist to provide a guide on usage.

The initial goal is to SEED a database with Nodes and Relationships, currently this works with the Sample. Next goal is simple queries to allow access to your database e.g. via an api. Later, it will also handle a 'live' graph database, essentially CRUD type operations (I don't have this use case yet)


## Why bother with a graph DB?
One reason (there are many)...if you're doing any AI, graph DB's can improve results with [graphRAG](https://neo4j.com/blog/genai/what-is-graphrag/).


## Getting Started
The star class is the Neo4jGenericRepo. With this class at the core, I've tried to make it as simple as possible to get Nodes and Relationships created (Upserted). You can decorate properties in your Node classes with an attribute to indicate they should be written as properties of the Node. Relationships are created in a similar way, by decorating a property (IEnumberable) with an attribute.

### Neo4j Database
You're going to need one of these to connect to.

Best place to start is [auraDB](https://neo4j.com/product/auradb/), provided by Neo4j. It has a free tier (with node and relationship limits).

When you do this, go to "Instances" and copy the ID (not the name). Use this value to set Neo4jSettings:Connection in appsettings.json (replace "localhost").

Or, you can run it locally in Docker:
```
$neo4jpassword = "YourPassword"
$today = Get-Date -Format "yyyyMMdd"
# start neo4j
docker run -d --rm `
  --name neo4j-$today `
  -e NEO4J_AUTH=neo4j/$neo4jpassword `
  -e NEO4J_dbms_memory_heap_initial__size=512m `
  -e NEO4J_dbms_memory_heap_max__size=1G `
  -v C:/Projects/YourNeo4jProject/data:/data `
  -p 7474:7474 `
  -p 7687:7687 `
  neo4j:latest
```
Once your nodes and relationships have been created, run this query to see all nodes and relationships (including nodes without relationships)
```
MATCH (n)
OPTIONAL MATCH (n)-[r]-(m)
RETURN n, r, m
```

### .Net project setup

#### Clone the repository

```
git clone https://github.com/wallism/Neo4jLiteRepo.git
cd Neo4jLiteRepo
```

#### Copy Neo4jListRepo

However you want to achieve this technically is up to you, but you will need the Neo4jRepo project in your solution. A nuget package is on the todo list (a fair way down).

I'd recommend just copy and paste it into your solution structure, especially while you're getting used to the functionality, you'll want to be able to debug through the code.

When you're first getting started, it's probably worth copying the .Importer project too, then replacing the Sample code with your code.

#### Create your own Neo4j project

Not entirely necessary, but definitely recommended. You could add the required files in any appropriate project in your sln, but this keeps all of your Neo4j logic easily referenceable.

Follow the design of the .Sample.csproj (the data folder doesn't *have* to be nested in the project, the path is configurable).

### How do I create nodes?
Look at the .Sample project for examples and guidance.

There are 3 things you need to do to create a Node:
* Create a Node class in the Label folder (inherits from GraphNode, see Movie and Genre in the sample)
* Create a NodeService in the NodeServices folder
* Register all Node services in your IoC container, e.g. 
```
builder.Services.AddSingleton<INodeService, MovieNodeService>();
```
* In your node clases, add NodePropertyAttribute's to properties you want included in your graph node.

### How do I create Relationships?
In your node class, add a NodeRelationshipAttribute to a property that has relationship info. The property needs to be an IEnumerable<< string>>.

You can define as many relationships as you need.

An item in the array needs to be the "primary key" value of the related node. In the sample, the Movie class has a Genres field and in the json, this is a comma separated array.

This will probably require some maninpulation on your part to ensure your Node data conforms with this structure, but it's worth that (relatively small?) effort to make relationship creation so easy.

### Config and Application Startup
See appsettings.json and Program.cs in the .Importer project. 

The .Importer project uses [Serilog](https://github.com/serilog/serilog) for logging, but this is the only project that has the dependency. Use whatever logger you prefer.


### Notes
#### Primary Key's
Neo4j doesn't really have a concept of a primary key, I'm just using this familiar terminology. Essentially the PK of a Node is an ideal field to use for searches. 

The PK field should be unique (this is enforced by default with a unique constraint, but you can turn that off if you need - not recommended because any duplicates found when creating relationships, will just use the first one).


#### Why do we really need NodeService?
I tried to avoid the need for these but the reflection code got pretty scary and every approach I tried didn't quite work. Generics mixed reflection is a recipe for a headache! Anyway, I think as the library evolves, the NodeServices will become more useful...

#### Docker
There is a dockerfile but I haven't been using...i.e. it is untested.

## Contributing

Contributions are welcome! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

If you have any questions, feel free to reach out.

Happy coding!