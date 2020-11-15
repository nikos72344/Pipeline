import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IReader;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

//Класс модуля чтения данных

public class Reader implements IReader {
    private static Logger LOGGER;   // - ссылка на логгер

    private final static String[] tokens = {"SIZE_TO_READ"};    // - токен конфига модуля чтения

    private enum tokenInd {SIZE_TO_READ}    // - индекс токена

    private IExecutable producer;   // - ссылка на производителя
    private IExecutable consumer;   // - ссылка на потребителя

    private FileInputStream fis;    // - поток чтения

    private String configFileName;  // - имя файла конфига
    private Map<String, String> map;    // - словарь с содержимым конфига

    private int sizeToRead; // - размер порции чтения

    //Конструктор

    public Reader(Logger logger) {
        LOGGER = logger;
    }

    //Установка производителя

    public RC setProducer(IExecutable p) {
        producer = p;
        LOGGER.info("Producer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потребителя

    public RC setConsumer(IExecutable c) {
        if (c == null || !(c instanceof Executor)) {
            LOGGER.severe("Wrong consumer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        consumer = c;
        LOGGER.info("Consumer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потока чтения

    public RC setInputStream(FileInputStream fis) {
        this.fis = fis;
        LOGGER.info("Input stream is set");
        return RC.CODE_SUCCESS;
    }

    //Метод, проверящий и обрабатывающий значения токенов в конфиге

    private RC dataValidation() {
        try {   //Преобразовние строки в целое значение
            sizeToRead = Integer.parseInt(map.get(tokens[tokenInd.SIZE_TO_READ.ordinal()]));
        } catch (NumberFormatException e) {   // - обработка исключения неверного значения
            LOGGER.severe("Invalid " + tokens[tokenInd.SIZE_TO_READ.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        if (sizeToRead < 1) {   // - проверка допустимого диапазона значений
            LOGGER.severe("Invalid " + tokens[tokenInd.SIZE_TO_READ.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        LOGGER.info(tokens[tokenInd.SIZE_TO_READ.ordinal()] + " value is valid");
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и обработки конфига модуля чтения

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
        code = dataValidation();    // - обработка содержимого конфига
        if (code != RC.CODE_SUCCESS)
            return code;
        LOGGER.info("Reader config file read successfully");
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

    //Метод выполненяющий чтения данных, а также запуск модуля потребителя

    public RC execute(byte[] data) {
        RC code = RC.CODE_SUCCESS;
        int flag = 0;
        while (true) {
            byte[] buffer = new byte[sizeToRead];   // - буфер для порции байтов
            try {
                flag = fis.readNBytes(buffer, 0, sizeToRead);   // - чтение
            } catch (IOException e) {   // - обработка исключения
                LOGGER.severe("Couldn't read data from input file");
                return RC.CODE_FAILED_TO_READ;
            }
            if (flag < sizeToRead && flag != 0) {    // - обработка случая чтения неполной порции байтов
                LOGGER.severe("Incomplete data");
                return RC.CODE_FAILED_TO_READ;
            }
            if (flag == 0) {    // - обработка случая достижения конца файла
                LOGGER.info("All the data was read successfully");
                return consumer.execute(null);
            }
            LOGGER.info("Portion data was read successfully");
            code = consumer.execute(buffer);    // - запуск модуля потребителя
            if (code != RC.CODE_SUCCESS)
                return code;
        }
    }
}
