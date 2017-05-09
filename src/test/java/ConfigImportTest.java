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
 * Created by eladw on 09/03/2017.
 */
public class ConfigImportTest {

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
    public void testImportWithConfig() {

        // setup
        //TODO: create configuration file with paths to the files in the resources directory.

        // when

        // then

    }

    @Test
    public void testNodesIndex() throws URISyntaxException {

        //NOTE: to run this test, the import config file in the resources folder should be edited,
        //  to point to the correct path for the data files in your machine (the resources folder full path).

        //setup
        URL resource = ConfigImportTest.class.getResource("import_config_test.json");
        String path = Paths.get(resource.toURI()).toAbsolutePath().toString().replace("\\", "/");
        String cypher  = String.format("call org.dragons.neo4j.procs.loadWithConfiguration('%s', 10)", path);

        //when
        graphDatabaseService.execute(cypher);

        //then
        Result result = graphDatabaseService.execute("match (:person) return count(*) as n");
        Object n = Iterators.single(result.columnAs("n"));
        Assert.assertEquals(12, ((Long)n).intValue());

        result = graphDatabaseService.execute("match (:person)-[k:knows]->(:person) return count(k) as k");
        Object k = Iterators.single(result.columnAs("k"));
        Assert.assertEquals(12, ((Long)k).intValue());
    }
}
