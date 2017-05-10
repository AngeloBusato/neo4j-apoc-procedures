package apoc.word.analyzer;

/**
 * Created by larusba on 5/9/17.
 */

import apoc.util.TestUtil;
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

    private static final String TEXT = "The reopening of the case has sent the Clinton campaign into complete chaos according to reports . Recommended Donald Trump has overtaken his political rival Hillary Clinton in a major new poll out this weekend as mainstream media outlets have refused to publish the results to the public. (53 mins ago) ";

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, WordAnalyzerProcedure.class);

        params = new HashMap<>();
        params.put("text", TEXT);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testGetSentences() {
        String call = "CALL apoc.word.analyzer.sentences({text}) yield result";
        TestUtil.testCall(db, call, params, r -> {
            System.out.println("sentences " + r.get("result"));
        });
    }

    @Test
    public void testGetTokens() {
        String call = "CALL apoc.word.analyzer.tokens({text}) yield result";
        TestUtil.testCall(db, call, params, r -> {
            System.out.println("tokens " + r.get("result"));
        });
    }

    @Test
    public void testNames() {
        params.put("type", "person");
        String call = "CALL apoc.word.analyzer.names({text}, {type}) yield result";
        TestUtil.testCall(db, call, params, r -> {
            System.out.println("name: " + r.get("result"));
        });
    }
}
