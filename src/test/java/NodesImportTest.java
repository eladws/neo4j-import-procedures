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

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by eladw on 13/03/2017.
 */
public class NodesImportTest {

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
        URL resource = NodesImportTest.class.getResource("persons1.csv");
        String path = Paths.get(resource.toURI()).toAbsolutePath().toString().replace("\\", "/");
        String cypher  = String.format("call org.dragons.neo4j.procs.loadNodesFile('%s', 'person', 'id:int,name:string,age:int', true, 2, null)", path);

        // when
       graphDatabaseService.execute(cypher);

        // then
        final Result result = graphDatabaseService.execute("match (:person) return count(*) as n");

        Object n = Iterators.single(result.columnAs("n"));

        //resource file persons1.csv contains 6 nodes
        Assert.assertEquals(6, ((Long)n).intValue());

    }

}
