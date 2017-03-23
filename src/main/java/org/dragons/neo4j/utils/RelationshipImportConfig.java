package org.dragons.neo4j.utils;


/**
 * Created by User on 23/03/2017.
 */
public class RelationshipImportConfig {
    public String rootDir;
    public String namePattern;
    public String label;
    public String startNodeLabel;
    public String startNodeMatchPropName;
    public String endNodeLabel;
    public String endNodeMatchPropName;
    public String header;
    public boolean skipFirst;
}
