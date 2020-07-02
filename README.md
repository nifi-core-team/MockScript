# MockScript
The project goal is to be an exact copy of the NiFi environment for testing and debugging of groovy scripts
that should be executable in `ExecuteScript` and `ExecuteGroovyScript` processors

 * paste groovy script into /nifi-mock-processors/src/main/resources/script.groovy
 * paste data into /nifi-mock-processors/src/main/resources/source
 * run test `testProcessor()` from `com.tinkoff.processors.mock`
 * check output `/nifi-mock-processors/target/out`
 
 
