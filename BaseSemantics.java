import ru.spbstu.pipeline.BaseGrammar;
import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

//Класс, вызывающий парсер конфига, а также проводящий семантическую проверку содежимого файла

public class BaseSemantics extends BaseGrammar implements IConfigurable {
    private static Logger LOGGER;   // - ссылка на логгер

    protected final static int wordsNum = 2;    // - значение количества слов в стоке

    private String configFileName;  // - имя конфигурационного файла
    protected ArrayList<ArrayList<String>> data;  // - контейнер разделенных на слова строк
    protected Map<String, String> map;    // - словарь, хранящий иформацию: токен - значение

    //Конструктор

    public BaseSemantics(Logger logger, String[] tokens) {
        super(tokens);  // - вызов конструктора абстрактного родительского класса
        LOGGER = logger;  // - установка соответствующего логгера
    }

    //Установка имени файла конфига

    public RC setConfig(String configFileName) {
        this.configFileName = configFileName;
        LOGGER.info("Config file name is set");
        return RC.CODE_SUCCESS;
    }

    //Метод, вызывающий парсер конфига

    public RC readConfig() {
        Parser parser = new Parser(LOGGER); // - создание экземпляра парсера
        parser.setConfig(configFileName);   // - установка имени конфига для класса парсера
        parser.setDelimiter(delimiter());   // - установка разделителя
        RC code = parser.run(); // - запуск чтения и парсинга файла
        if (code != RC.CODE_SUCCESS)
            return code;
        data = parser.getStrings(); // - получение строк, разделенных на слова
        LOGGER.info("Config file read successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, заполняющий словарь данными

    protected RC fillMap(ArrayList<String> arr) {
        if (arr.size() != wordsNum) {   // - проверка на наличие токена и одного значения в строке
            LOGGER.severe("Token has more than one value");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        map.put(arr.get(0), arr.get(wordsNum - 1)); // - занесение соответствующего токена и его значения в словарь
        return RC.CODE_SUCCESS;
    }

    //Метод, производящий семантический анализ содержимого конфигурационного файла

    public RC run() {
        if (data.size() != numberTokens()) {    // - проверка колчества строк в файле
            LOGGER.severe("Wrong number of tokens");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        map = new HashMap<String, String>();    // - создание словаря для хранения данных
        for (int i = 0; i < numberTokens(); i++) {
            for (ArrayList<String> arr : data) {
                if (arr.get(0).equals(token(i))) {
                    RC code = fillMap(arr);
                    if (code != RC.CODE_SUCCESS)
                        return code;
                    break;
                }
            }
        }
        if (map.size() != numberTokens()) { // - соотношение количества требуемых и прочитанных токенов
            LOGGER.severe("Invalid token");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        LOGGER.info("Config file tokens are valid");
        return RC.CODE_SUCCESS;
    }

    //Метод, возвращающий словарь

    public Map<String, String> getMap() {
        return map;
    }
}
