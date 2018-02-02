Для работы с проектом необходим установленный на машине Python 3

Для запуска проекта на UNIX-подобной ОС (проверялось на MacOSX), необходимо перейти в консоль и выполнить несколько команд: 
1. git clone https://github.com/jedemdasseine/MTS.git
2. cd MTS/
3. sudo sh DB.sh

После этого заходим в DB Browser for SQLite (http://sqlitebrowser.org), подключаемся к базе MTSDB.db и смотрим результаты.



Для повторной генерации данных в таблицу CHECK_OBJECT(старое не затирает, новое добавляет новыми строками) нужно запустить в папке с проектом команду:

python3 generate.py --count 1000000 --output MTSDB.db

где параметр --count отвечает за количество сгенерированных строк с данными, а параметр --output, за путь до рабочей базы данных. По умолчанию из shell скрипта база данных создается в папке с проектом.
По умолчанию эмулируется генерация данных за 5 последних дней.

Для повторного подсчета статистики с новыми данными нужно запустить в папке с проектом команду:

python3 calc_stats.py --db MTSDB.db

где параметр --db отвечает за путь к рабочей базе данных, с таблицей CHECK_OBJECT по которой считается статистика.
Подсчет метрик производится на данных датированных сегодняшним числом, соответственно при наступлении каждого нового дня, данные нужно генерировать заново.
