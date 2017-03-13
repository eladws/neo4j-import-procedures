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
 * Created by User on 13/03/2017.
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
    public void testSanityFileImport() {

        // setup

        // when
        graphDatabaseService.execute("create (:dragon {name: 'John'})");
        graphDatabaseService.execute("create (:dragon {name: 'Jane'})");
        graphDatabaseService.execute("call org.dragons.neo4j.procs.loadRelationshipFile('C:/data/rels.txt', 'fire', 'dragon', 'dragon', 'name', 'name',0,1,null, false, 2)");

        // then
        final Result result = graphDatabaseService.execute("match (:dragon)-[:fire]->(:dragon) return count(*) as n");

        Object n = Iterators.single(result.columnAs("n"));

        Assert.assertEquals(1, ((Long)n).intValue());

    }

}
