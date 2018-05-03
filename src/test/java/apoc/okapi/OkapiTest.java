package apoc.okapi;

import apoc.util.TestUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.bag.HashBag;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.apocGraphDatabaseBuilder;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

@SuppressWarnings( "Duplicates" )
public class OkapiTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = apocGraphDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Okapi.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testOkapiSchemaForSingleLabel() {
        db.execute(
                "CREATE (:A {val1: 'String', val2: 1})" +
                   "CREATE (:A {val1: 'String', val2: 1.2})"
        ).close();

        testResult(db, "CALL apoc.okapi.schema", (result) -> {
            HashBag<Map<String, Object>> results = result.stream().collect( Collectors.toCollection( HashBag::new ) );

            HashBag<Map<String, Object>> expected = new HashBag<>();
            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A"));
                put("property", "val1");
                put("cypherType", "STRING");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A"));
                put("property", "val2");
                put("cypherType", "NUMBER");
            }});

            assertEquals( expected, results );
        });
    }

    @Test
    public void testOkapiSchemaForSingleMultipleLabels() {
        db.execute(
                   "CREATE (:A {val1: 'String'})" +
                   "CREATE (:B {val2: 2})" +
                   "CREATE (:A:B {val1: 'String', val2: 2})"
        ).close();

        testResult(db, "CALL apoc.okapi.schema", (result) -> {
            HashBag<Map<String, Object>> results = result.stream().collect( Collectors.toCollection( HashBag::new ) );

            HashBag<Map<String, Object>> expected = new HashBag<>();
            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A"));
                put("property", "val1");
                put("cypherType", "STRING");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("B"));
                put("property", "val2");
                put("cypherType", "INTEGER");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A", "B"));
                put("property", "val1");
                put("cypherType", "STRING");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A", "B"));
                put("property", "val2");
                put("cypherType", "INTEGER");
            }});

            assertEquals( expected, results );
        });
    }

    @Test
    public void testOkapiSchemaForLabelWithEmptyLabel() {
        db.execute("CREATE ({val1: 'String'})").close();

        testResult(db, "CALL apoc.okapi.schema", (result) -> {
            HashBag<Map<String, Object>> results = result.stream().collect( Collectors.toCollection( HashBag::new ) );

            HashBag<Map<String, Object>> expected = new HashBag<>();
            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList());
                put("property", "val1");
                put("cypherType", "STRING");
            }});

            assertEquals( expected, results );
        });
    }

    @Test
    public void testOkapiSchemaForLabelWithoutProperties() {
        db.execute("CREATE (:A)").close();

        testResult(db, "CALL apoc.okapi.schema", (result) -> {
            HashBag<Map<String, Object>> results = result.stream().collect( Collectors.toCollection( HashBag::new ) );

            HashBag<Map<String, Object>> expected = new HashBag<>();
            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A"));
                put("property", "");
                put("cypherType", "");
            }});

            assertEquals( expected, results );
        });
    }

    @Test
    public void testOkapiSchemaForSingleRelationship() {
        db.execute(
                   "CREATE (a:A)" +
                   "CREATE (b:A)" +
                   "CREATE (a)-[:REL {val1: 'String', val2: true}]->(b)" +
                   "CREATE (a)-[:REL {val1: 'String', val2: 2.0}]->(b)"
        ).close();

        testResult(db, "CALL apoc.okapi.schema", (result) -> {
            HashBag<Map<String, Object>> results = result.stream().collect( Collectors.toCollection( HashBag::new ) );

            HashBag<Map<String, Object>> expected = new HashBag<>();
            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A"));
                put("property", "");
                put("cypherType", "");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Relationship");
                put("labels", Lists.newArrayList("REL"));
                put("property", "val1");
                put("cypherType", "STRING");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Relationship");
                put("labels", Lists.newArrayList("REL"));
                put("property", "val2");
                put("cypherType", "ANY");
            }});

            assertEquals( expected, results );
        });
    }

    @Test
    public void testOkapiSchemaForRelationshipWithoutProperties() {
        db.execute(
                "CREATE (a:A)" +
                "CREATE (b:A)" +
                "CREATE (a)-[:REL]->(b)"
        ).close();

        testResult(db, "CALL apoc.okapi.schema", (result) -> {
            HashBag<Map<String, Object>> results = result.stream().collect( Collectors.toCollection( HashBag::new ) );

            HashBag<Map<String, Object>> expected = new HashBag<>();
            expected.add( new HashMap<String, Object>() {{
                put("type", "Node");
                put("labels", Lists.newArrayList("A"));
                put("property", "");
                put("cypherType", "");
            }});

            expected.add( new HashMap<String, Object>() {{
                put("type", "Relationship");
                put("labels", Lists.newArrayList("REL"));
                put("property", "");
                put("cypherType", "");
            }});

            assertEquals( expected, results );
        });
    }

}
