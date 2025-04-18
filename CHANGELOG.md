## v2.0.1
* исправлено некорректное поведение при указании через флаг JSON файла-источника
* добавлен лог с количеством строк в `materialized view` для источника данных `db`
* обновлены поля логов с прогрессом:
  * `mps` округляется до целых в меньшую сторону
  * добавлено поле `total published` с общим количеством обработанных записей
* обновлены зависимости
## v2.0.0
* утилита переписана
  * миграция на библиотеки `isp-kit` и `grmq`
  * утилита написана на основе библиотеки `cli`
  * реализована команда для генерации схемы конфига рядом с бинарником
  * добавлен другая версия источника `json` для работы с директорией, содержащую множество json файлов по одному сообщению
  * добавлены режимы логирования и опциональное логирование публикуемых сообщений
  * реализованы ретраи при публикации в очередь 
  * добавлена поддержка `rate limiter` для публикации в очередь  
  * для источников данных `json` и `rmq` добавлен режим `plain-text`, позволяющий отправлять данные байтами без десереализации
  * изменен алгоритм вычитки данных из базы данных
  * добавлен Dockerfile для возможности установки утилиты через образ
## v1.5.0
* обновлены зависимости
* добавлен логер
* добавлены функции toolkit
## v1.4.4
* upgrade dependencies
## v1.4.3
* fix GC
## v1.4.1
* fix script timeout error (upgrade isp-lib)
## v1.4.0
* add uuid generation function
## v1.3.1
* fix wrap body of script by `function(){` ... `}`
## v1.3.0
* migrate from inbuilt wrapping of goja to new isp-lib/scripts package
## v1.2.6
* fix db source with parallel enabled and where clause in query
## v1.2.5
* add setting comma for csv reader
## v1.2.4
* add quoting `'` in estimating query
## v1.2.3
* fix an endless loop after error
* increase script timeout from 200ms to 1000ms
* remove source.db.cursor param; always use cursors
## v1.2.0
* add rabbitmq as data source
* update dependencies
## v1.1.2
* omit nil results from script
## v1.1.0
* add alternative concurrent db data sources
