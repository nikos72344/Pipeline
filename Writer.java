import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IWriter;
import ru.spbstu.pipeline.RC;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

//Класс буфера для записи

class Buffer {
    private byte[] buffer;  // - буфер
    private int filled; // - количество занятых ячеек
    FileOutputStream fos;   // - поток для записи

    //Метод установки потока для записи

    public RC setOutputStream(FileOutputStream fos) {
        this.fos = fos;
        return RC.CODE_SUCCESS;
    }

    //Метод установки буфера определенного размера

    public RC setBuffer(int size) {
        buffer = new byte[size];
        return RC.CODE_SUCCESS;
    }

    //Провера на заполненность

    public boolean isFull() {
        return filled == buffer.length;
    }

    //Добавление в буффер еще одного значения

    public boolean add(byte val) {
        if (!isFull()) { // - проверка на заполненность
            buffer[filled] = val;   // - добавление при наличии свободных ячеек
            filled++;   // - увеличения счетчика занятых ячеек
        } else
            return false;   // - возврат при неуспешном добавлении
        return true;    // - возврат при успешном добавлении
    }

    //Запись буффера в файл

    public RC write() {
        try {
            fos.write(buffer, 0, filled);   // - запись
            filled = 0;   // - обнуление счетчика занятых ячеек
        } catch (IOException e) { // - обработка исключения
            return RC.CODE_FAILED_TO_WRITE;
        }
        return RC.CODE_SUCCESS;
    }
}

//Класс модуля записи данных в файл

public class Writer implements IWriter {
    private static Logger LOGGER;   // - ссылка на логгер

    private final static String[] tokens = {"SIZE_TO_WRITE"};   // - токен конфига модуля записи

    private enum tokenInd {SIZE_TO_WRITE}   // - индекс токена

    private IExecutable producer;   // - ссылка на производителя
    private IExecutable consumer;   // - ссылка на потребителя

    private FileOutputStream fos;   // - поток записи

    private String configFileName;  // - имя файла конфига
    private Map<String, String> map;    // - словарь с содержимым конфига

    private int sizeToWrite;    // - размер буфера для записи
    private Buffer buffer;  // - буфер для записи

    //Конструктор

    public Writer(Logger logger) {
        LOGGER = logger;
    }

    //Установка производителя

    public RC setProducer(IExecutable p) {
        if (p == null || !(p instanceof Executor)) {
            LOGGER.severe("Wrong producer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        producer = p;
        LOGGER.info("Producer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потребителя

    public RC setConsumer(IExecutable c) {
        consumer = c;
        LOGGER.info("Consumer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потока записи

    public RC setOutputStream(FileOutputStream fos) {
        RC code;
        this.fos = fos;
        code = buffer.setOutputStream(fos);
        LOGGER.info("Output stream is set");
        return code;
    }

    //Метод, проверящий и обрабатывающий значения токенов в конфиге

    public RC dataValidation() {
        try {   //Преобразовние строки в целое значение
            sizeToWrite = Integer.parseInt((map.get(tokens[tokenInd.SIZE_TO_WRITE.ordinal()])));
        } catch (NumberFormatException e) {   // - обработка исключения неверного значения
            LOGGER.severe("Invalid " + tokens[tokenInd.SIZE_TO_WRITE.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        if (sizeToWrite < 1) {  // - проверка допустимого диапазона значений
            LOGGER.severe("Invalid " + tokens[tokenInd.SIZE_TO_WRITE.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        LOGGER.info(tokens[tokenInd.SIZE_TO_WRITE.ordinal()] + " value is valid");
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и обработки конфига модуля записи, а также создания буфера

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
        buffer = new Buffer();    // - создание экземпляра буфера
        code = buffer.setBuffer(sizeToWrite); // - создание буфера определенного размера
        if (code != RC.CODE_SUCCESS)
            return code;
        LOGGER.info("Writer config file read successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод установки имени конфига, работы с ним и с буфером

    public RC setConfig(String configFileName) {
        if (configFileName == null) {
            LOGGER.severe("Null pointer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        this.configFileName = configFileName;
        LOGGER.info("Config file name is set");
        return readConfig();
    }

    //Метод, выполняющий заполнение буфера и своевременную запись его содержимого в файл

    public RC execute(byte[] data) {
        RC code = RC.CODE_SUCCESS;
        if (data == null) {    // - обработка случая достижения конца файла
            LOGGER.info("Writing the remaining data");
            return buffer.write();  // - запись оставшихся данных в файл
        }
        LOGGER.info("Saving data into buffer");
        for (int i = 0; i < data.length; i++) {
            if (!buffer.add(data[i])) {  // - попытка добавления байта в буфер
                code = buffer.write();    // - при невозможности запись содержимого буфера в файл
                if (code != RC.CODE_SUCCESS) {
                    LOGGER.severe("Couldn't write data to output file");
                    return code;
                }
                LOGGER.info("Writing data from the buffer");
                LOGGER.info("Saving data into buffer");
                buffer.add(data[i]);    // - добавление байта в пустой буфер
            }
        }
        return RC.CODE_SUCCESS;
    }
}
