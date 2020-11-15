import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

//Класс менеджера

public class Manager implements IConfigurable {
    private final static Logger LOGGER = Logger.getLogger(Manager.class.getName()); // - создание логгера

    private final static String[] tokens = {"READER", "EXECUTOR", "WRITER", "INPUT", "OUTPUT"}; // - токены конфига мессенджера

    private enum tokenInd {READER, EXECUTOR, WRITER, INPUT, OUTPUT} // - индексы для токенов

    private String configFileName;  // - имя файла конфига
    private Map<String, String> map;    // - словарь с содержимым конфига

    private Reader reader;  // - класс модуля чтения
    private Executor executor;  // - класс модуля обработки
    private Writer writer;  // - класс модуля записи

    //Метод чтения и обработки конфига метода

    private RC readConfig() {
        BaseSemantics sem = new BaseSemantics(LOGGER, tokens);  // - создание экземпляра класса семантической обработки
        sem.setConfig(configFileName);  // - установка конфига менеджера
        RC code = sem.readConfig(); // - чтение и парсинг конфига
        if (code != RC.CODE_SUCCESS)
            return code;
        code = sem.run();   // - проведение семантического анализа
        if (code != RC.CODE_SUCCESS)
            return code;
        map = sem.getMap(); // - получение обработанного содержимого конфига
        LOGGER.info("Manager config file read successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод установки имени конфига, а также работы с ним

    public RC setConfig(String configFileName) {
        if (configFileName == null) {
            LOGGER.severe("Null pointer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        this.configFileName = configFileName;
        LOGGER.info("Config file name is set");
        return readConfig();
    }

    //Метод настройки конвеера

    public RC setPipeline() {
        RC code = RC.CODE_SUCCESS;

        reader = new Reader(LOGGER);    // - создания экземпляра модуля чтения
        executor = new Executor(LOGGER);    // - создание экземпляра модуля обработки
        writer = new Writer(LOGGER);    // - создание модуля записи

        code = reader.setProducer(null);    // - установка производителя для модуля чтения
        if (code != RC.CODE_SUCCESS)
            return code;
        code = reader.setConsumer(executor);    // - установка потребителя для модуля чтения
        if (code != RC.CODE_SUCCESS)
            return code;
        code = reader.setConfig(map.get(tokens[tokenInd.READER.ordinal()]));    // - установка соответствующего конфига модулю чтения
        if (code != RC.CODE_SUCCESS)
            return code;

        code = executor.setProducer(reader);    // - установка производителя для модуля обработки
        if (code != RC.CODE_SUCCESS)
            return code;
        code = executor.setConsumer(writer);    // - установка потребителя для модуля обработки
        if (code != RC.CODE_SUCCESS)
            return code;
        code = executor.setConfig(map.get(tokens[tokenInd.EXECUTOR.ordinal()]));    // - установка соответствующего конфига модулю обработки
        if (code != RC.CODE_SUCCESS)
            return code;

        code = writer.setProducer(executor);    // - установка производителя для модуля записи
        if (code != RC.CODE_SUCCESS)
            return code;
        code = writer.setConsumer(null);    // - установка потребителя для модуля записи
        if (code != RC.CODE_SUCCESS)
            return code;
        code = writer.setConfig(map.get(tokens[tokenInd.WRITER.ordinal()]));    // - установка соответствующего конфига модулю записи
        if (code != RC.CODE_SUCCESS)
            return code;
        LOGGER.info("Pipeline was created successfully");
        return code;
    }

    //Метод, открывающий и закрывающий потоки чтения/записи, а также запускающий конвеер

    public RC run() {
        FileInputStream fis;    // - поток чтения
        FileOutputStream fos;   // - поток записи
        RC code = RC.CODE_SUCCESS;

        try {
            fis = new FileInputStream(map.get(tokens[tokenInd.INPUT.ordinal()]));   // - открытие потока чтения
            code = reader.setInputStream(fis);  // - передача его в модуль чтени
        } catch (FileNotFoundException e) { // - обработка исключения
            LOGGER.severe("Input stream is invalid");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        if (code != RC.CODE_SUCCESS)
            return code;

        try {
            fos = new FileOutputStream(map.get(tokens[tokenInd.OUTPUT.ordinal()])); // - открытие потока записи
            code = writer.setOutputStream(fos); // - передача его в модуль записи
        } catch (FileNotFoundException e) { // - обработка исключения
            LOGGER.severe("Output stream is invalid");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }
        if (code != RC.CODE_SUCCESS)
            return code;

        code = reader.execute(null);    // - запуск конвеера

        try {
            fis.close();    // - закрытие потока чтения
        } catch (IOException e) {   // - обработка исключения
            LOGGER.severe("Input stream is invalid");
            code = RC.CODE_INVALID_INPUT_STREAM;
        }
        try {
            fos.close();    // - закртыие потока записи
        } catch (IOException e) {   // - обработка исключения
            LOGGER.severe("Output stream is invalid");
            code = RC.CODE_INVALID_OUTPUT_STREAM;
        }
        return code;
    }
}
