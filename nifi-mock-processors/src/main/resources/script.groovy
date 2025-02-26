/*
*
* Paste here your groovy script
*
* This script is an example
* */



import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets

// Логирование: начало выполнения скрипта
log.info("Groovy script started")

// Получаем текущий FlowFile
def in_ff = session.get()
if (!in_ff) {
    log.error("No FlowFile found")
    return
}

// Получаем динамическое свойство процессора
def propertyName1 = 'DynamicProperty1'
def propertyValue1 = context.getProperty(propertyName1)?.getValue()
def propertyName2 = 'DynamicProperty2'
def propertyValue2 = context.getProperty(propertyName2)?.getValue()

// Логирование: проверка атрибутов FlowFile
log.info("FlowFile attributes: ${in_ff.getAttributes()}")

// Чтение содержимого FlowFile
def content = ""
session.read(in_ff, { inputStream ->
    content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8)
} as InputStreamCallback)

// Логирование: проверка содержимого FlowFile
log.info("FlowFile content: ${content}")

// Парсинг JSON
def jsonSlurper = new JsonSlurper()
def parsedJson = jsonSlurper.parseText(content)

// Получаем значение поля "login"
def login = parsedJson.login ?: "default_login"

// Формируем содержимое файла с атрибутами и значением login
def outputText = """FlowFile Attributes:
${in_ff.getAttributes().entrySet().join("\n")}

JSON атрибут:
login = ${login}

Dynamic Property1:
${propertyValue1}
Dynamic Property2:
${propertyValue2}
"""

// Создаем новый FlowFile с результатом
def out_ff = session.create(in_ff)
out_ff = session.write(out_ff, { outputStream ->
    outputStream.write(outputText.getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)

// Передаем FlowFile в отношение SUCCESS
session.transfer(out_ff, REL_SUCCESS)

// Удаляем исходный FlowFile
session.remove(in_ff)

// Логирование: завершение выполнения скрипта
log.info("Groovy script finished successfully")
