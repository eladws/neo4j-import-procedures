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

/**
 * Created by eladw on 09/03/2017.
 */
public class DataDirImportTest {

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
    public void testSingleRowImport() {

        // setup

        // when
        graphDatabaseService.execute("call org.dragons.neo4j.procs.loadWithConfiguration('C:/Dev/BitBucket/neo4j-import/src/test/resources/import_config_test.json',4)");

        // then
        final Result result = graphDatabaseService.execute("match (:dragon)-[:fire]->(:dragon) return count(*) as n");

        Object n = Iterators.single(result.columnAs("n"));

        // Assert.assertEquals(20, ((Long)n).intValue());

        final Result result2 = graphDatabaseService.execute("match (d:dragon) return d.name as name");

        Object name = Iterators.array(result2.columnAs("name"))[0];

        Assert.assertEquals("Dragonov", (String)name);
    }

}
