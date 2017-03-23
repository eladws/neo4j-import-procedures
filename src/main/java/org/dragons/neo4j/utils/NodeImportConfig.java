package org.dragons.neo4j.utils;

import java.util.List;

/**
 * Created by User on 23/03/2017.
 */
public class NodeImportConfig {
    public String rootDir;
    public String namePattern;
    public String label;
    public String header;
    public boolean skipFirst;
    public List<String> indexedProps;
}
