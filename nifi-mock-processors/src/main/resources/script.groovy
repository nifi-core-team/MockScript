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

int buffer_size = 8096
def out_ff = session.create(in_ff)

try{
    session.read(in_ff, {inputStream ->
        BufferedInputStream bis=new BufferedInputStream(inputStream, buffer_size)
        OutputStream os = session.write(out_ff)
        os.close()
        BufferedOutputStream bos = new BufferedOutputStream(os, buffer_size)
        JsonSlurper slurper = new JsonSlurper()
        bis.eachLine { line ->
            def rec = slurper.parseText(line)
            bos.write(JsonOutput.toJson(rec).getBytes(StandardCharsets.UTF_8))
            bos.write('\n'.getBytes(StandardCharsets.UTF_8))
        }

        bis.close()
        bos.flush()
        bos.close()

    } as InputStreamCallback)

    out_ff = session.putAttribute(out_ff, "attr", "some_value")
    session.transfer(out_ff, REL_SUCCESS)
    session.remove(in_ff)
} catch(IOException e){
    session.transfer(in_ff, REL_FAILURE)
    session.remove(out_ff)
}