package org.dragons.neo4j.config;

/**
 * Created by Elad on 3/28/2017.
 */
public abstract class BaseImportConfig {
    public String rootDir;
    public String namePattern;
    public String label;
    public String header;
    public boolean skipFirst;

}
