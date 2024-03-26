/*
*
* Paste here your groovy script
*
* This script is an example
* */


import groovy.json.JsonOutput
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.io.InputStreamCallback
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets

session = (ProcessSession)session
def in_ff = session.get()
if(!in_ff) return
session.remove(in_ff)