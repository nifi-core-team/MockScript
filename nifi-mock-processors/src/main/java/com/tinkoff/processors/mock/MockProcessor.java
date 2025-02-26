package com.tinkoff.processors.mock;

import groovy.lang.GroovyCodeSource;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Tags({"example", "groovy", "mock"})
@CapabilityDescription("A mock processor that executes a Groovy script and supports dynamic properties.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@DynamicProperty(
        name = "The name of the dynamic property",
        value = "The value of the dynamic property",
        description = "Allows users to define custom properties for the processor."
)
public class MockProcessor extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to this relationship on success.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship on failure.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        // Поддержка любых динамических свойств
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Dynamic property: " + propertyDescriptorName)
                .required(false)
                .addValidator((subject, input, context) -> {
                    // Всегда возвращаем успешный результат валидации
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(true) // Указываем, что значение валидно
                            .build();
                })
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Можно добавить дополнительную логику при запуске процессора
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        GroovyShell shell = new GroovyShell(this.getClass().getClassLoader());
        File scriptFile = new File(
                getClass().getClassLoader().getResource("script.groovy").getFile()
        );

        // Логирование всех свойств процессора
        processContext.getProperties().forEach((key, value) -> {
            getLogger().info("Property: {} = {}", key.getName(), value);
        });

        // Собираем только динамические свойства
        Map<String, String> dynamicProperties = processContext.getProperties()
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().isDynamic()) // Фильтруем только динамические свойства
                .collect(Collectors.toMap(
                        entry -> entry.getKey().getName(),
                        entry -> entry.getValue()
                ));

        // Передаем переменные в Groovy-скрипт
        shell.setVariable("session", processSession);
        shell.setVariable("context", processContext);
        shell.setVariable("log", getLogger());
        shell.setVariable("REL_SUCCESS", SUCCESS);
        shell.setVariable("REL_FAILURE", FAILURE);
        shell.setVariable("dynamicProperties", dynamicProperties); // Передаем динамические свойства

        Script script = null;
        try {
            script = shell.parse(scriptFile);
        } catch (IOException e) {
            throw new ProcessException("Failed to parse Groovy script", e);
        }
        script.run();
    }
}
