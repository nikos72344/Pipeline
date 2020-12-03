import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IPipelineStep;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

//Расширенный класс семантического разбора для менеджера

class ManagerSemantics extends BaseSemantics {
    private static Logger LOGGER;   // - ссылка на логгер

    private String orderToken;  // - токен порядка установки модулей конвейера

    private String[] order; // - массив порядка установки модулей

    //Конструктор

    public ManagerSemantics(Logger logger, String[] tokens, String orderToken) {
        super(logger, tokens);
        LOGGER = logger;
        this.orderToken = orderToken;
    }

    //Функция, заполняющая порядок установки модулей

    private RC fillOrder(ArrayList<String> arr) {
        if (arr.size() < wordsNum) {    // - конвейер не будет работать без единого модуля
            LOGGER.severe("Wrong amount of executors");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        order = new String[arr.size() - 1];
        for (int j = 1; j < arr.size(); j++) {
            order[j - 1] = arr.get(j);
        }
        return RC.CODE_SUCCESS;
    }

    //Функция, производящая семантический анализ, обрабатывающая токен порядка модулей конвейера

    public RC run() {
        RC code = RC.CODE_SUCCESS;
        if (data.size() != numberTokens() + 1) {    // - проверка колчества строк в файле
            LOGGER.severe("Wrong number of tokens");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        map = new HashMap<String, String>();    // - создание словаря для хранения данных
        for (int i = 0; i < numberTokens(); i++) {
            for (ArrayList<String> arr : data) {
                if (order == null && arr.get(0).equals(orderToken)) {
                    code = fillOrder(arr);  // - обработка случая токена порядка модулей
                    if (code != RC.CODE_SUCCESS)
                        return code;
                }
                if (arr.get(0).equals(token(i))) {
                    code = fillMap(arr);    // - обработка остальных случаев
                    if (code != RC.CODE_SUCCESS)
                        return code;
                    if (order != null)
                        break;
                }
            }
        }
        if (order == null) {    // - проверка наличия токена порядка модулей в конфиге
            LOGGER.severe("Order token is missing");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        if (map.size() != numberTokens()) { // - соотношение количества требуемых и прочитанных токенов
            LOGGER.severe("Invalid token");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        LOGGER.info("Config file tokens are valid");
        return RC.CODE_SUCCESS;
    }

    //Метод, возвращающий массив текстовых представлений модулей

    public String[] getOrder() {
        return order;
    }
}

//Класс менеджера

public class Manager implements IConfigurable {
    private final static Logger LOGGER = Logger.getLogger(Manager.class.getName()); // - создание логгера

    private final static String[] tokens = {"READER", "EXECUTOR", "WRITER", "INPUT", "OUTPUT"}; // - токены конфига мессенджера
    private final static String orderToken = "ORDER";   // - токен порядка модулей конвейера
    private final static int moduleNum = 3; // - количество модулей конвейера

    private enum tokenInd {READER, EXECUTOR, WRITER, INPUT, OUTPUT} // - индексы для токенов

    private String configFileName;  // - имя файла конфига
    private Map<String, String> map;    // - словарь с содержимым конфига
    private Queue<IPipelineStep> queue; // - очередь модулей конвейера
    private String[] order; // - текстовое представление порядка модулей конвейера

    private IExecutable starter;    // - стартовый модуль

    private FileInputStream fis;    // - поток чтения
    private FileOutputStream fos;   // - поток записи

    private Reader reader;  // - класс модуля чтения
    private Executor executor;  // - класс модуля обработки
    private Writer writer;  // - класс модуля записи

    //Метод чтения и обработки конфига метода

    private RC readConfig() {
        ManagerSemantics sem = new ManagerSemantics(LOGGER, tokens, orderToken);  // - создание экземпляра класса семантической обработки
        sem.setConfig(configFileName);  // - установка конфига менеджера
        RC code = sem.readConfig(); // - чтение и парсинг конфига
        if (code != RC.CODE_SUCCESS)
            return code;
        code = sem.run();   // - проведение семантического анализа
        if (code != RC.CODE_SUCCESS)
            return code;
        map = sem.getMap(); // - получение обработанного содержимого конфига
        order = sem.getOrder();   // - получение текстового предстваления порядка модулей конвейера
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

    //Метод обработки текстовых значений модулей и создания очереди подключения модулей

    private RC setQueue(String[] order) {
        if (order.length != moduleNum) {  // - проверка количества модулей
            LOGGER.severe("Wrong amount of pipeline modules");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }

        queue = new LinkedList<IPipelineStep>();    // - создание очереди

        for (String module : order) {  //Проверка наличия модуля чтения
            if (module.toLowerCase().equals(Reader.class.getSimpleName().toLowerCase())) {
                if (reader != null) {
                    LOGGER.severe("Reader module already exists");
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                }
                reader = new Reader(LOGGER);    // - создание модуля чтения
                queue.offer(reader);    // - добавление в очередь
                continue;
            }   //Проверка наличия модуля обработки
            if (module.toLowerCase().equals(Executor.class.getSimpleName().toLowerCase())) {
                if (executor != null) {
                    LOGGER.severe("Executor module already exists");
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                }
                executor = new Executor(LOGGER);  // - создание модуля обработки
                queue.offer(executor);  // - добавление в очередь
                continue;
            }   //Проверка наличия модуля записи
            if (module.toLowerCase().equals(Writer.class.getSimpleName().toLowerCase())) {
                if (writer != null) {
                    LOGGER.severe("Writer module already exists");
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                }
                writer = new Writer(LOGGER);  // - создание модуля записи
                queue.offer(writer);    // - добавление в очередь
                continue;
            }   //Обработка случая нераспознанного модуля конвейера:
            LOGGER.severe("Unrecognized module");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        LOGGER.info("Queue is set");
        return RC.CODE_SUCCESS;
    }

    //Метод, устанавливающий конфиг для модуля чтения, открывающий и передающий ему поток чтения

    private RC setReader() {
        RC code = RC.CODE_SUCCESS;

        code = reader.setConfig(map.get(tokens[tokenInd.READER.ordinal()]));    // - установка соответствующего конфига модулю чтения
        if (code != RC.CODE_SUCCESS)
            return code;

        try {
            fis = new FileInputStream(map.get(tokens[tokenInd.INPUT.ordinal()]));   // - открытие потока чтения
            code = reader.setInputStream(fis);  // - передача его в модуль чтени
        } catch (FileNotFoundException e) { // - обработка исключения
            LOGGER.severe("Input stream is invalid");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        return code;
    }

    //Метод, устанавливающий конфиг модулю обработки

    private RC setExecutor() {
        return executor.setConfig(map.get(tokens[tokenInd.EXECUTOR.ordinal()]));    // - установка соответствующего конфига модулю обработки
    }

    //Метод, устанавливающий конфиг модулю записи, а также открывающий и передающйи поток для записи

    private RC setWriter() {
        RC code = RC.CODE_SUCCESS;

        code = writer.setConfig(map.get(tokens[tokenInd.WRITER.ordinal()]));    // - установка соответствующего конфига модулю записи
        if (code != RC.CODE_SUCCESS)
            return code;

        try {
            fos = new FileOutputStream(map.get(tokens[tokenInd.OUTPUT.ordinal()])); // - открытие потока записи
            code = writer.setOutputStream(fos); // - передача его в модуль записи
        } catch (FileNotFoundException e) { // - обработка исключения
            LOGGER.severe("Output stream is invalid");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }
        return code;
    }

    //Установка модулей на конвейер

    private RC setModules() {
        RC code = RC.CODE_SUCCESS;
        IPipelineStep producer = null, module, consumer;
        while (true) {
            try {
                module = queue.remove();    // - извлечение модуля из очереди
                consumer = queue.peek();    // - получение потребителя модуля без извлечения

                if (producer == null)
                    starter = module; // - установка стартового модуля

                code = module.setProducer(producer);  // - установка производителя
                if (code != RC.CODE_SUCCESS)
                    return code;
                code = module.setConsumer(consumer);  // - установка потребителя
                if (code != RC.CODE_SUCCESS)
                    return code;

                producer = module;  // - установка актуального модуля производителем
            } catch (NoSuchElementException e) {  // - обработка случая пустой очереди
                break;
            }
        }
        LOGGER.info("Pipeline set successfully");
        return code;
    }

    //Метод настройки конвеера

    public RC setPipeline() {
        RC code = RC.CODE_SUCCESS;

        code = setQueue(order); // - создание очереди
        if (code != RC.CODE_SUCCESS)
            return code;

        code = setModules();    // - "сборка" конвейера
        if (code != RC.CODE_SUCCESS)
            return code;

        code = setReader(); // - настройка модуля чтения
        if (code != RC.CODE_SUCCESS)
            return code;

        code = setExecutor();   // - настройка модуля обработки
        if (code != RC.CODE_SUCCESS)
            return code;

        code = setWriter(); // - настройка модуля записи
        if (code != RC.CODE_SUCCESS)
            return code;

        return code;
    }

    //Метод, запускающий конвеер, а также закрывающий потоки чтения/записи

    public RC run() {
        RC code = RC.CODE_SUCCESS;

        code = starter.execute(null);    // - запуск конвейера

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
