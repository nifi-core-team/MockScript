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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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
        TestRunner runner = TestRunners.newTestRunner(MockProcessor.class);

        // Чтение общих атрибутов из default.attributes (если файл существует)
        Map<String, String> defaultAttributes = new HashMap<>();
        File defaultAttributesFile = new File(sourceDir, "default.attributes");
        if (defaultAttributesFile.exists()) {
            ObjectMapper objectMapper = new ObjectMapper();
            defaultAttributes = objectMapper.readValue(defaultAttributesFile, Map.class);
            System.out.println("Default attributes: " + defaultAttributes);
        }

        // Чтение динамических свойств из dynamic-properties.json (если файл существует)
        File dynamicPropertiesFile = new File(sourceDir, "dynamic-properties.json");
        if (dynamicPropertiesFile.exists()) {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> dynamicProperties = objectMapper.readValue(dynamicPropertiesFile, Map.class);

            // Передаем динамические свойства в runner
            dynamicProperties.forEach((key, value) -> runner.setProperty(key, value));

            // Логирование: проверка динамических свойств
            System.out.println("Dynamic Properties: " + dynamicProperties);
        }

        // Получаем список всех файлов из sourceDir, исключая служебные файлы и файлы с суффиксом .attributes
        List<Path> sourcePaths = Arrays.stream(sourceDir.listFiles())
                .map(File::toPath)
                .filter(path -> !path.getFileName().toString().endsWith(".attributes") &&
                        !path.getFileName().toString().equals("dynamic-properties.json"))
                .collect(Collectors.toList());

        if (sourcePaths.isEmpty()) {
            throw new FileNotFoundException("No valid files found in " + sourceDir.getAbsolutePath());
        }

        // Добавляем каждый файл в очередь с уникальными атрибутами
        for (Path path : sourcePaths) {
            Map<String, String> attributes = new HashMap<>();

            // Генерация уникальных дефолтных атрибутов для каждого файла
            String randomUUID = UUID.randomUUID().toString();
            attributes.put("filename", randomUUID); // Уникальное имя файла
            attributes.put("uuid", randomUUID);     // Уникальный UUID
            attributes.put("path", "./");
            attributes.put("entryDate", String.valueOf(System.currentTimeMillis()));
            attributes.put("lineageStartDate", String.valueOf(System.currentTimeMillis()));
            attributes.put("fileSize", String.valueOf(Files.size(path)));

            // Добавление общих атрибутов
            attributes.putAll(defaultAttributes);

            // Чтение специфических атрибутов для текущего файла (если файл существует)
            String attributeFileName = path.getFileName().toString() + ".attributes";
            File specificAttributesFile = new File(sourceDir, attributeFileName);
            if (specificAttributesFile.exists()) {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, String> specificAttributes = objectMapper.readValue(specificAttributesFile, Map.class);

                // Специфические атрибуты имеют приоритет над общими
                attributes.putAll(specificAttributes);

                System.out.println("Specific attributes for " + path.getFileName() + ": " + specificAttributes);
            }

            // Сохраняем оригинальное имя файла в атрибутах
            attributes.put("originalFilename", path.getFileName().toString());

            // Логирование: проверка содержимого файла
            String fileContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            System.out.println("Enqueuing file: " + path.getFileName() + ", Content: " + fileContent);

            // Добавляем файл в очередь процессора
            runner.enqueue(path, attributes);
        }

        // Запускаем процессор для всех файлов в очереди
        int fileCount = sourcePaths.size();
        runner.run(fileCount); // Указываем количество итераций равным количеству файлов

        // Обработка результатов
        List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MockProcessor.SUCCESS);
        Class<MockFlowFile> mockFlowFileClass = MockFlowFile.class;
        Method method = mockFlowFileClass.getDeclaredMethod("getData");
        method.setAccessible(true);

        // Сохраняем успешные файлы с уникальными именами
        successFiles.stream()
                .map(file -> new FileToWrite(
                        file.getAttribute("filename"), // Используем уникальное имя файла на основе UUID
                        getData(file, method),
                        file.getAttributes()
                ))
                .forEach(fileToWrite -> fileToWrite.writeTo(successDir.toPath()));

        // Сохраняем файлы с ошибками
        List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(MockProcessor.FAILURE);
        failureFiles.stream()
                .map(file -> new FileToWrite(
                        file.getAttribute("filename"), // Используем уникальное имя файла на основе UUID
                        getData(file, method),
                        file.getAttributes()
                ))
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
            // Используем переданное имя файла
            File file = path.resolve(this.filename).toFile();

            if (!file.exists()) {
                try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file))) {
                    outputStream.write(this.content);
                    outputStream.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
