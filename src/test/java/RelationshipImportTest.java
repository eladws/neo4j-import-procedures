import org.dragons.neo4j.procs.ImportProcedures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Created by eladw on 13/03/2017.
 */
public class RelationshipImportTest {

    private GraphDatabaseService graphDatabaseService;

    @Before
    public void setup() throws KernelException {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final Procedures procedures = ((GraphDatabaseAPI) graphDatabaseService).getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(ImportProcedures.class);
        procedures.registerFunction(ImportProcedures.class);
    }

    @After
    public void teardown() {
        graphDatabaseService.shutdown();
    }

    @Test
    public void testSanityFileImport() throws URISyntaxException {

        // setup
        for(int i=0; i<20; i++) {
            String cypher = String.format("create (:person {id:%d})",i);
            graphDatabaseService.execute(cypher);
        }

        URL resource = NodesImportTest.class.getResource("knows1.csv");
        String path = Paths.get(resource.toURI()).toAbsolutePath().toString().replace("\\", "/");
        String cypher  = String.format("call org.dragons.neo4j.procs.loadRelationshipsFile('%s', 'knows', 'person', 'person', 'id', 'id', 'start:int,end:int,since:string', true, 2)", path);

        // when
        graphDatabaseService.execute(cypher);

        // then
        final Result result = graphDatabaseService.execute("match (:person)-[k:knows]->(:person) return count(k) as n");

        Object n = Iterators.single(result.columnAs("n"));

        //resource file knows1.csv contains 6 relationships
        Assert.assertEquals(6, ((Long)n).intValue());

    }

}
