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

import java.util.List;

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
    public void testSingleRowImport() {

        // setup

        // when
        graphDatabaseService.execute("call org.dragons.neo4j.procs.loadWithConfiguration('C:/Dev/Github/neo4j-import-procedures/src/test/resources',2)");

        // then
        final Result result = graphDatabaseService.execute("match (:person)-[:friend]->(:person) return count(*) as n");

        Object n = Iterators.asList(result.columnAs("n"));

        Assert.assertTrue(((List) n).size() > 0);

    }

}
