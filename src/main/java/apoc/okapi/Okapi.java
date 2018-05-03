package apoc.okapi;

import apoc.Pools;
import apoc.result.OkapiSchemaInfo;
import apoc.util.Util;
import com.google.common.collect.Lists;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.opencypher.okapi.api.schema.Schema$;
import org.opencypher.okapi.api.types.CypherType;
import org.opencypher.okapi.api.value.CypherValue;
import org.opencypher.okapi.api.value.CypherValue$;
import scala.Tuple2;
import scala.collection.JavaConversions$;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Okapi {
    @Context
    public GraphDatabaseService db;

    @Context
    public KernelTransaction tx;

    @Procedure(value = "apoc.okapi.schema", mode = Mode.SCHEMA)
    @Description("CALL apoc.okapi.schema yield type, labels, property, cypherType - Returns schema information of this graph in Okapi format.")
    public Stream<OkapiSchemaInfo>schema() {
        return constructOkapiSchemaInfo();
    }

    /**
     * Computes the schema of the Neo4j graph as used by Okapi
     *
     * @return
     */
    private Stream<OkapiSchemaInfo> constructOkapiSchemaInfo() {
        ExecutorService pool = Pools.DEFAULT;
        List<Future<org.opencypher.okapi.api.schema.Schema>> futures = new ArrayList<>(1000);

        ResourceIterator<Node> nodes = db.getAllNodes().iterator();

        while ( nodes.hasNext() )
        {
            List<Node> batch = Util.take( nodes, 10000 );
            futures.add( Util.inTxFuture( pool, db, () -> batch.stream()
                    .map( node -> {
                        scala.collection.immutable.List<Tuple2<String,CypherType>> scalaTypes =
                                extractPropertyTypes( node.getAllProperties() );

                        List<String> labelSet = StreamSupport
                                .stream( node.getLabels().spliterator(), false )
                                .map( Label::name )
                                .collect( Collectors.toList() );
                        scala.collection.immutable.List<String> scalaSet = toScalaList( labelSet );

                        return Schema$.MODULE$.empty()
                                .withNodePropertyKeys( scalaSet, scalaTypes );
                    } ).reduce( Schema$.MODULE$.empty(), org.opencypher.okapi.api.schema.Schema::$plus$plus ) ) );
        }

        org.opencypher.okapi.api.schema.Schema nodeSchema = collect(futures);

        futures.clear();

        ResourceIterator<Relationship> relationships = db.getAllRelationships().iterator();
        while ( relationships.hasNext() )
        {
            List<Relationship> batch = Util.take( relationships, 10000 );
            futures.add( Util.inTxFuture( pool, db, () -> batch.stream()
                    .map( relationship -> {
                        scala.collection.immutable.List<Tuple2<String,CypherType>> scalaMap =
                                extractPropertyTypes( relationship.getAllProperties() );

                        return Schema$.MODULE$.empty()
                                .withRelationshipPropertyKeys( relationship.getType().name(), scalaMap );
                    } ).reduce( Schema$.MODULE$.empty(), org.opencypher.okapi.api.schema.Schema::$plus$plus ) ) );
        }
        org.opencypher.okapi.api.schema.Schema relationshipSchema = collect(futures);

        Stream<OkapiSchemaInfo> nodeRes =
                toJavaMap( nodeSchema.labelPropertyMap().map() ).entrySet().stream().flatMap( entry -> {
                    java.util.List<String> labelCombo = Lists.newArrayList( toJavaSet( entry.getKey() ));
                    return getOkapiSchemaInfo( "Node", labelCombo, toJavaMap( entry.getValue() ) );
                } );

        Stream<OkapiSchemaInfo> relRes =
                toJavaMap( relationshipSchema.relTypePropertyMap().map() ).entrySet().stream().flatMap( entry -> {
                    java.util.List<String> labelCombo = Lists.newArrayList(entry.getKey());
                    return getOkapiSchemaInfo( "Relationship", labelCombo, toJavaMap(entry.getValue()) );
                } );

        return Stream.concat( nodeRes, relRes );
    }

    /**
     * Waits for all futures to finish and collects their result
     *
     * @param futures schema computation futures
     * @return
     */
    private org.opencypher.okapi.api.schema.Schema collect(
            List<Future<org.opencypher.okapi.api.schema.Schema>> futures
    ) {
        return futures.stream()
                .map(f -> {
                    try { return f.get(); }
                    catch ( InterruptedException | ExecutionException e ) {
                        e.printStackTrace();
                        return Schema$.MODULE$.empty();
                    }} )
                .reduce(  Schema$.MODULE$.empty(), org.opencypher.okapi.api.schema.Schema::$plus$plus );
    }

    /**
     * Generates the OkapiSchemaInfo entries for a given label combination / relationship type
     *
     * @param type identifies the created entries (Label or Relationship)
     * @param labels label combination / relationship type for which the property keys are computed
     * @param propertyKeyMap propertyKeys for the given labels/ relationship type
     * @return
     */
    private Stream<OkapiSchemaInfo> getOkapiSchemaInfo( String type, List<String> labels,
            Map<String,CypherType> propertyKeyMap
    ) {
        if (propertyKeyMap.isEmpty()) {
            return Stream.of( new OkapiSchemaInfo( type, labels, "", "" ) );
        } else {
            return propertyKeyMap
                    .entrySet().stream()
                    .map( propertyAndType -> new OkapiSchemaInfo(
                            type,
                            labels,
                            propertyAndType.getKey(),
                            propertyAndType.getValue().toString() ) );
        }
    }

    /**
     * Extracts the property types from the properties of Node/Relationship
     *
     * @param allProperties property map of a Node/Relationship
     * @return
     */
    private scala.collection.immutable.List<Tuple2<String,CypherType>> extractPropertyTypes(
            Map<String,Object> allProperties
    ) {
        java.util.List<Tuple2<String,CypherType>> javaTypes = allProperties
                .entrySet().stream()
                .map( e -> new Tuple2<>( e.getKey(), getCypherType( e.getValue() ) ) )
                .collect( Collectors.toList() );

        return JavaConversions$.MODULE$.asScalaBuffer( javaTypes ).toList();
    }

    /**
     * Computes the Cypher Type for a given property value
     *
     * @param value property value
     * @return
     */
    private CypherType getCypherType(Object value) {
        CypherValue.CypherValue cypherValue = CypherValue$.MODULE$.apply(value);
        return new CypherType.TypeCypherValue(cypherValue).cypherType();
    }

    /**
     * Converts a Java List into a Scala List
     *
     * @param javaList input Java list
     * @param <V> inner type of the list
     * @return
     */
    private <V> scala.collection.immutable.List<V> toScalaList(java.util.List<V> javaList) {
        return JavaConversions$.MODULE$.asScalaBuffer( javaList ).toList();
    }

    /**
     * Converts a Scala map into a Java map
     *
     * @param scalaMap input Scala map
     * @param <K> Key type
     * @param <V> Value type
     * @return
     */
    private <K,V> java.util.Map<K,V> toJavaMap(scala.collection.Map<K, V> scalaMap) {
        return JavaConverters.mapAsJavaMapConverter( scalaMap ).asJava();
    }

    /**
     * Converts a Scala set into a Java set
     *
     * @param scalaSet input scala set
     * @param <V> inner type of the set
     * @return
     */
    private <V> java.util.Set<V> toJavaSet(scala.collection.Set<V> scalaSet) {
        return JavaConverters.setAsJavaSetConverter( scalaSet ).asJava();
    }
}
