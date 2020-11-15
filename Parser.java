import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RC;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.Logger;

//Класс, производящий чтение и синтаксический парсинг строк

public class Parser implements IConfigurable {
    private static Logger LOGGER;   // - ссылка на логгер

    private static String delimiter;    // - значение разделителя
    private static String space = " ";  // - значение пробела
    private static String nullString = "";  // - значение пустой строки

    private String configFileName;  // - имя конфига для парсинга
    private ArrayList<String> rawData;  // - строки, не разделенные на слова, конфига
    private ArrayList<ArrayList<String>> data;  // - разделенные на слова строки конфига

    //Конструктор, устанавливающий соответствующий логгер

    public Parser(Logger logger) {
        LOGGER = logger;
    }

    //Установка имени файла конфига

    public RC setConfig(String configFileName) {
        this.configFileName = configFileName;
        LOGGER.info("Config file name is set");
        return RC.CODE_SUCCESS;
    }

    //Установка значения разделителя

    public RC setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        LOGGER.info("Delimiter is set");
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и разбиения на строки файла конфига

    private RC read() {
        rawData = new ArrayList<String>();  // - создание конейнера для хранения строк
        try {   //Открытие файла конфига для построчного чтения:
            File file = new File(configFileName);
            FileReader fileReader = new FileReader(file);
            BufferedReader buffReader = new BufferedReader(fileReader);
            String str = buffReader.readLine(); // - чтение строки из файла
            while (str != null) {   //Добавление строки в контейнер и последующее чтение до тех пор, пока не достигнем конца файла:
                rawData.add(str);
                str = buffReader.readLine();
            }
            //Закрытие потоков чтения
            buffReader.close();
            fileReader.close();
        } catch (Exception e) { // - обработка исключений, возникающих при работе с файлом
            LOGGER.severe("Config file is not opened");
            return RC.CODE_CONFIG_GRAMMAR_ERROR;
        }
        LOGGER.info("Config file read successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, производящий деление строк на слова

    private RC parse() {
        data = new ArrayList<ArrayList<String>>();  // - контейнер для хранения строк, поделенных на слова
        for (String str : rawData) {
            data.add(new ArrayList<String>());  // - добавление новой строки в контейнер
            String[] one = str.split(delimiter);    // - разделение строк при наличии разделителя
            for (String i : one) {
                String[] two = i.trim().split(space);   // - разделения строк по пробелу
                for (String j : two) {
                    if (!j.equals(nullString))  // - добавление слов в строку, исключая пустые строки
                        data.get(data.size() - 1).add(j);
                }
            }
        }
        LOGGER.info("Config file parsed successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, запускающий чтение и парсинг конфига

    public RC run() {
        RC code = read();
        if (code != RC.CODE_SUCCESS)
            return code;
        return parse();
    }

    //Метод, возвращающий разделенные на слова строки

    public ArrayList<ArrayList<String>> getStrings() {
        return data;
    }
}
