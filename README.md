# neo4j-import-procedures
### Noe4j procedures for loading data from CSV files

A collection of procedures to parse CSV files, and insert nodes and relationships into neo4j.

It can be used to update an existing online database.

You can control the size of each transaction, and add indexing on the relevant properties to improve performance.

## Usage:

**Import nodes file:**

```javascript
CALL org.dragons.neo4j.procs.loadNodesFile('my_file.csv','myNodeLabel','name1:type1,name2:type2',false,1000,[name1])
```

**Import relationships file:**

```javascript
CALL org.dragons.neo4j.procs.loadRelationshipFile('my_file.csv','myRelLabel','startNodeLabel','endNodeLabel','startNodeProperty','endNodeProperty','start:int,end:string,name1:type1,name2:type2',false,1000,true)
```
