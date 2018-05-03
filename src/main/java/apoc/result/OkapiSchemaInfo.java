package apoc.result;

import java.util.List;

public class OkapiSchemaInfo {
    /**
     * Indicates whether the entry is a node or a relationship
     */
    public final String type;
    /**
     * A combination of labels or a relationship
     */
    public final List<String> labels;
    /**
     * A property that occurs on the given label combination / relationship type
     */
    public final String property;
    /**
     * The CypherType of the given property on the given label combination / relationship type
     */
    public final String cypherType;

    public OkapiSchemaInfo( String type, List<String> labels, String property, String cypherType ) {
        this.type = type;
        this.labels = labels;
        this.property = property;
        this.cypherType = cypherType;
    }
}
