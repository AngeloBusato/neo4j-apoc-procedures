package apoc.word.analyzer;

/**
 * Created by larusba on 5/9/17.
 */
import apoc.word.analyzer.WordAnalyzerProcedure;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import org.neo4j.graphdb.GraphDatabaseService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;

/**
 * Created by alberto.delazzari on 09/05/17.
 */
public class WordAnalyzerProcedureTest {

    private static GraphDatabaseService db;

    private static Map<String, Object> params;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure(db, WordAnalyzerProcedure.class);

        params = new HashMap<>();
        params.put("text", "Alberto De Lazzari");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    private static void testCall(String call) {
        Consumer<Map<String, Object>> consumer = r -> {
            System.out.println(r);
        };
        testResult(db, call, params, (res) -> {
            try {
                if (res.hasNext()) {
                    Map<String, Object> row = res.next();
                    consumer.accept(row);
                }
                assertFalse(res.hasNext());
            } catch (Throwable t) {
                System.out.println(t.getLocalizedMessage());
                throw t;
            }
        });
    }

    private static void registerProcedure(GraphDatabaseService db, Class<?>... procedures) throws KernelException {
        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        for (Class<?> procedure : procedures) {
            proceduresService.registerProcedure(procedure);
            proceduresService.registerFunction(procedure);
        }
    }

    public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }

    @Test
    public void testGetSentmences() {
        String call = "CALL apoc.word.analyzer.sentences({text}) yield result";
        testCall(call);
    }
}
