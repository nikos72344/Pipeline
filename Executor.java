import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.RC;

import java.util.Map;
import java.util.logging.Logger;

//Класс, выполнящий циклический сдвиг

public class Executor implements IExecutor {
    private static Logger LOGGER;   // - ссылка логгер

    private final static String[] tokens = {"SHIFT_AMOUNT", "SHIFT_DIRECTION"}; // - токены конфига модуля обработки

    private enum tokenInd {SHIFT_AMOUNT, SHIFT_DIRECTION}   // - индексы токенов

    private final static int BYTE_SIZE = 8; // - размер байта в битах

    private IExecutable producer;   // - ссылка на производителя
    private IExecutable consumer;   // - ссылка на потребителя

    private String configFileName;  // - имя файла конфига
    private Map<String, String> map;    // - словарь с содержимым конфига

    //Принимаемые значения направления сдвига

    private final static String[] wordVal = {"left", "right"};
    private final static String[] numberVal = {"-1", "1"};

    private enum Direction {LEFT, RIGHT}    // - предстваление для храения направления сдвига

    private int shiftAmount;    // - величина сдвига
    private Direction shiftDirection;   // - направление сдвига

    //Конструктор

    public Executor(Logger logger) {
        LOGGER = logger;
    }

    //Установка производителя

    public RC setProducer(IExecutable p) {
        if (p == null || !(p instanceof Reader)) {
            LOGGER.severe("Wrong producer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        producer = p;
        LOGGER.info("Producer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потребителя

    public RC setConsumer(IExecutable c) {
        if (c == null || !(c instanceof Writer)) {
            LOGGER.severe("Wrong consumer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        consumer = c;
        LOGGER.info("Consumer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, проверящий и обрабатывающий значения токенов в конфиге

    public RC dataValidation() {
        try {   //Преобразовние строки в целое значение
            shiftAmount = Integer.parseInt(map.get(tokens[tokenInd.SHIFT_AMOUNT.ordinal()]));
        } catch (NumberFormatException e) {   // - обработка исключения неверного значения
            LOGGER.severe("Invalid " + tokens[tokenInd.SHIFT_AMOUNT.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        if (shiftAmount < 1) {  // - проверка допустимого диапазона значений
            LOGGER.severe("Invalid " + tokens[tokenInd.SHIFT_AMOUNT.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }

        //Установка направления сдвига

        if (map.get(tokens[tokenInd.SHIFT_DIRECTION.ordinal()]).toLowerCase().equals(wordVal[shiftDirection.LEFT.ordinal()]) || map.get(tokens[tokenInd.SHIFT_DIRECTION.ordinal()]).equals(numberVal[shiftDirection.LEFT.ordinal()]))
            shiftDirection = Direction.LEFT;
        else if (map.get(tokens[tokenInd.SHIFT_DIRECTION.ordinal()]).toLowerCase().equals(wordVal[shiftDirection.RIGHT.ordinal()]) || map.get(tokens[tokenInd.SHIFT_DIRECTION.ordinal()]).equals(numberVal[shiftDirection.RIGHT.ordinal()]))
            shiftDirection = Direction.RIGHT;
        else {  //Обработка случая неверного значения направления сдвига
            LOGGER.severe("Invalid " + tokens[tokenInd.SHIFT_DIRECTION.ordinal()] + " value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        LOGGER.info("Executor values are valid");
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и обработки конфига модуля обработки

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
        LOGGER.info("Executor config file read successfully");
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

    //Циклический побитовый сдвиг влево

    private byte shiftLeft(byte data) {
        int x = data & 0xFF;
        int y = shiftAmount % BYTE_SIZE;
        return (byte) ((x << y) | (x >> (BYTE_SIZE - y)));
    }

    //Циклический побитовый сдвиг вправо

    private byte shiftRight(byte data) {
        int x = data & 0xFF;
        int y = shiftAmount % BYTE_SIZE;
        return (byte) ((x >> y) | (x << (BYTE_SIZE - y)));
    }

    //Метод, производящий циклический побитовый сдвиг

    private byte[] doShift(byte[] data) {
        if (data == null) { // - обработка случая достижения конца файла
            LOGGER.info("There is no data to shift");
            return null;
        }
        switch (shiftDirection) {
            case LEFT:  //Побитовый сдвиг влево
                for (int i = 0; i < data.length; i++)
                    data[i] = shiftLeft(data[i]);
                break;
            case RIGHT: //Побитовый сдвиг вправо
                for (int i = 0; i < data.length; i++)
                    data[i] = shiftRight(data[i]);
                break;
        }
        LOGGER.info("Data is shifted");
        return data;
    }

    //Метод, производящий циклический побитовый сдвиг и запуск модуля потребителя

    public RC execute(byte[] data) {
        return consumer.execute(doShift(data));
    }
}
