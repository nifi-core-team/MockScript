/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tinkoff.processors.mock;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MockProcessorTest {

    static final Path currentDir = Paths.get("").toAbsolutePath();

    public static final Path projectPath = currentDir; // test/java/com/tinkoff/processors/mock
    public static final Path targetPath = projectPath.resolve("target");
    public static final Path resourcesPath = projectPath.resolve("src")
            .resolve("main")
            .resolve("resources");

    public static final Path sourcePath = resourcesPath.resolve("source");
    public static final Path outPath = targetPath.resolve("out");
    public static final Path successPath = outPath.resolve("success");
    public static final Path failurePath = outPath.resolve("failure");

    public static File outDir = outPath.toFile();
    public static File sourceDir = sourcePath.toFile();
    public static File successDir = successPath.toFile();
    public static File failureDir = failurePath.toFile();

    @BeforeClass
    public static void createDirs() {
        if (!sourceDir.exists())
            sourceDir.mkdirs();
        if (!successDir.exists())
            successDir.mkdirs();
        if (!failureDir.exists())
            failureDir.mkdirs();
    }

    @Before
    public void cleanOutDir() {
        if(successDir.exists() && successDir.isDirectory() && successDir.list().length > 0){
            Arrays.asList(successDir.listFiles()).forEach(File::delete);
        }
        if(failureDir.exists() && failureDir.isDirectory() && failureDir.list().length > 0){
            Arrays.asList(failureDir.listFiles()).forEach(File::delete);
        }
    }


    /**
     * Method for starting script debugging
     * By default, a script is run from /nifi-mock-processors/src/main/resources/script.groovy
     * The script can be debugged either on FlowFile or on string lines
     * For debugging on FlowFile, just insert the file into /nifi-mock-processors/src/main/resources/source
     * the system automatically selects everything that is in the /source and put it in the queue
     * system put all results in /nifi-mock-processors/target/out
     * in success and failure, respectively
     */
    @Test
    public void testProcessor() throws NoSuchMethodException, IOException {
        TestRunner runner;
        runner = TestRunners.newTestRunner(MockProcessor.class);
        List<Path> sourcePaths = Arrays.stream(sourceDir.listFiles())
                .map(File::toPath)
                .collect(Collectors.toList());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("data.name", "dwh.dataname");
        for (Path path: sourcePaths){
            runner.enqueue(path, attributes);
        }

        runner.run();
        List<MockFlowFile> files = runner.getFlowFilesForRelationship(MockProcessor.SUCCESS);
        Class<MockFlowFile> mockFlowFileClass = MockFlowFile.class;
        Method method = mockFlowFileClass.getDeclaredMethod("getData");
        method.setAccessible(true);
        files.stream()
                .map(file -> new FileToWrite(String.valueOf(file.getId()), getData(file, method), null))
                .forEach(fileToWrite -> fileToWrite.writeTo(successDir.toPath()));
        List<MockFlowFile> errorFiles = runner.getFlowFilesForRelationship(MockProcessor.FAILURE);
        errorFiles.stream()
                .map(file -> new FileToWrite(String.valueOf(file.getId()), getData(file, method), null))
                .forEach(fileToWrite -> fileToWrite.writeTo(failureDir.toPath()));
    }

    public byte[] getData(MockFlowFile flowFile, Method method) {
        try {
            return (byte[]) method.invoke(flowFile);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public static class FileToWrite {
        public final String filename;
        public final byte[] content;
        public final Map<String, String> attrs;

        public FileToWrite(String filename, byte[] content, Map<String, String> attrs) {
            this.filename = filename;
            this.content = content;
            this.attrs = attrs;
        }

        public void writeTo(Path path) {
            File file = path.resolve(this.filename).toFile();
            if (!file.exists()) {
                try {
                    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
                    outputStream.write(this.content);
                    outputStream.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
