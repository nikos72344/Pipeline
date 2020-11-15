import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.util.logging.LogManager;

/*Описание требований к конфигурационномым файлам:
    Наличие всех соответствующих токенов и их значений конфигурационного файла;
    Возможно применение символа "=" для указания значения токена;
    Количество пробелов между словами не имеет значения;
    Регистр значения направления сдвига не влияет на работу;
    Порядок следования строк не имеет значения.

Список всех токенов:
    READER - конфиг для модуля чтения;
    EXECUTOR - конфиг для модуля обработки данных;
    WRITER - конфиг для модуля записи;
    INPUT - файл с данными для обработки;
    OUTPUT - файл с обработанными данными;
    SIZE_TO_READ - размер порции байтов для чтения;
    SHIFT_AMOUNT - величина циклического сдвига;
    SHIFT_DIRECTION - направление сдвига;
    SIZE_TO_WRITE - размер буфера данных для записи.

Принимаемые значения направления сдвига:
    left, -1 - циклический сдвиг влево;
    right, 1 - циклический сдвиг вправо.*/

public class Lab_2 {
    private final static String logConfig = "log.config";   // - имя конфигурационного файла для логгера

    public static void main(String[] args) {    // - точка входа
        try {    //Применение конфига к логгеру:
            LogManager.getLogManager().readConfiguration(new FileInputStream(logConfig));
        } catch (Exception e) {   // - обработка возникающих исключений
            System.err.println(logConfig + " is unavailable!");
            return;
        }
        if (args.length != 1) { // - обработка случая неверного количества переданных аргументов
            System.err.println("Wrong number of arguments!");
            return;
        }
        Manager manager = new Manager();    // - создание экземпляра менеджера
        RC code = manager.setConfig(args[0]);   // - установка конфига менеджера
        if (code != RC.CODE_SUCCESS)    // - прекращение работы в случае возникновения ошибки
            return;
        code = manager.setPipeline();   // - создание конвеера
        if (code != RC.CODE_SUCCESS)    // - прекращение работы в случае возникновения ошибки
            return;
        manager.run();  // - запуск конвеера
    }
}
