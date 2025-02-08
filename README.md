# MockScript

Цель проекта — быть точной копией среды NiFi для тестирования и отладки скриптов на Groovy, которые должны выполняться в процессорах ExecuteScript и ExecuteGroovyScript.

Отредактируйте скрипт на Groovy в файл `/nifi-mock-processors/src/main/resources/script.groovy`.

В каталоге `/nifi-mock-processors/src/main/resources/source` расположены два файла. 
`flowfile` это файл с входящими данными. 
`flowfile.attributes.json` файл с атрибутами флоуфайла в формате JSON перечеслены пары ключ - значение.

Запустите тест `testProcessor()` из пакета `com.tinkoff.processors.mock`.

![image](https://github.com/user-attachments/assets/b3c2898e-ad5d-4139-9256-e08c0e046163)

Если запустить в режиме debug, то можно пользоваться точками останова в groovy скрипте.
Пример:
![image](https://github.com/user-attachments/assets/9ba93d1b-1e17-422b-b6eb-b3b1ccc3580d)


Проверьте вывод в каталоге `/nifi-mock-processors/target/out`. 
В зависимости от результата либо в папке `success`, либо в `failure` появится выходной файл.


Если версия вашего JDK выше 1.8, то:
- Откройте конфигурацию запуска для `MockProcessorTest`.
- Найдите строку с параметрами виртуальной машины (VM options).
- Добавьте `"--add-opens java.base/java.lang=ALL-UNNAMED"`.
