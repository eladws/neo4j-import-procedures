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
        graphDatabaseService.execute("call org.dragons.neo4j.procs.importDataDirectory('C:/data', 2)");

        // then
        final Result result = graphDatabaseService.execute("match (:person) return count(*) as n");

        Object n1 = Iterators.single(result.columnAs("n"));

        final Result res = graphDatabaseService.execute("match (:dragon) return count(*) as n");

        Object n2 = Iterators.single(res.columnAs("n"));

        Assert.assertEquals(6,((Long)n1).intValue());

        Assert.assertEquals(2, ((Long)n2).intValue());

    }

}
