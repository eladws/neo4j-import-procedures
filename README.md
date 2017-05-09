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
CALL org.dragons.neo4j.procs.loadRelationshipsFile('my_file.csv','myRelLabel','startNodeLabel','endNodeLabel','startNodeProperty','endNodeProperty','start:int,end:string,name1:type1,name2:type2',false,1000)
```

**Load nodes and relationships with a configuration file**

```javascript
CALL org.dragons.neo4j.procs.loadWithConfiguration('config_file.json',10000)
```

## Parallelism options
Parallelism options are defined separately for nodes and relationships.

Four options are available to control the behaviour of the import process:
  1. "none": No parallelism at all. A single thread will read all files sequentialy.
  2. "group": A thread will be spawned for each nodes or relationships group. Within each group - the files will be processed           sequentialy.
  3. "in-group": The groups will be processed one after the other. The files inside each group will be processed in parallel.
  4. "all": All files will be processed in parallel (a thread for each file).

Note that regardless of the chosen parallelism level, the program waits until all the nodes threads to finish before starting to import edges.

**Example JSON configuration file**

```javascript
{
  "nodesParallelLevel" : "all",
  "relsParallelLevel" : "in-group",
  "nodes": [
    {
      "rootDir": "C:/nodesData",
      "namePattern": "**/nodes_person*",
      "label": "person",
      "header": "id:int,name:string",
      "skipFirst": false,
      "indexedProps": [name]
    },
    {
      "rootDir": "C:/nodesData",
      "namePattern": "**/nodes_actors*",
      "label": "actor",
      "header": "id:int,nickname:string",
      "skipFirst": false,
      "indexedProps": []
    }
  ],
  "relationships": [
    {
      "rootDir": "C:/relsData",
      "namePattern": "**/friend*",
      "label": "friend",
      "startNodeLabel": "person",
      "startNodeMatchPropName": "id",
      "endNodeLabel": "person",
      "endNodeMatchPropName": "id",
      "header": "start:int,end:int",
      "skipFirst": false
    }
  ]
}
```
