# IPRO12 Surgard Receiver Add-on (v2.1)

Расширенный приёмник SurGard / Contact ID для ИПРО-12.

- Поддержка формата STEMAX: `5000 18AAAAQXXXYYZZZ`
- Полный разбор Contact ID (Account, Qualifier, Code, Partition, Zone)
- Таблица кодов событий (RU, с возможностью выбора языка `lang: ru/en`, EN при отсутствии — fallback на RU)
- MQTT + MQTT Discovery
- Webhook (опционально)
- SQLite архив
- REST API + HTML-страница на порту 8124
