# MockScript
The project goal is to be an exact copy of the NiFi environment for testing and debugging of groovy scripts
that should be executable in `ExecuteScript` and `ExecuteGroovyScript` processors

 * paste groovy script into /nifi-mock-processors/src/main/resources/script.groovy
 * paste data into /nifi-mock-processors/src/main/resources/source
 * run test `testProcessor()` from `com.tinkoff.processors.mock`
 * if your JDK version above 1.8 than:
   * open run config for MockProcessorTest
   * find line with VM options
   * add "--add-opens java.base/java.lang=ALL-UNNAMED"
 * check output `/nifi-mock-processors/target/out`
 
 
