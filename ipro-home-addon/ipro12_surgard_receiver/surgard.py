import socket
import json
import os
import sqlite3
from datetime import datetime
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import paho.mqtt.client as mqtt
import requests

OPTIONS_PATH = "/data/options.json"
SURGARD_PORT = 6601
HTTP_PORT = 8124


def load_options():
    try:
        with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print("[IPRO12] Failed to load options.json:", e)
        return {}


opts = load_options()

USE_MQTT = bool(opts.get("use_mqtt", True))
MQTT_HOST = opts.get("mqtt_host", "core-mosquitto")
MQTT_PORT = int(opts.get("mqtt_port", 1883))
MQTT_USER = opts.get("mqtt_user", "homeassistant")
MQTT_PASS = opts.get("mqtt_pass", "password")
MQTT_BASE_TOPIC = opts.get("mqtt_base_topic", "ipro12")
DISCOVERY_PREFIX = opts.get("discovery_prefix", "homeassistant")

WEBHOOK_ENABLED = bool(opts.get("webhook_enabled", False))
WEBHOOK_URL = opts.get("webhook_url", "")

ARCHIVE_ENABLED = bool(opts.get("archive_enabled", True))
DB_PATH = "/data/ipro12_events.db"

SUPERVISION_TIMEOUT = int(opts.get("supervision_timeout", 300))
LANG = opts.get("lang", "ru").lower()

DB_CONN = None
STATE_LOCK = threading.Lock()
LAST_EVENT_TS = datetime.utcnow()
CONNECTION_STATE = "unknown"

ARMED = False
ALARM_ACTIVE = False
POWER_STATE = "unknown"
BATTERY_STATE = "unknown"


EVENT_CODES = {
    "100": {"ru": "Нажата Кнопка - Медицинская тревога"},
    "101": {"ru": "Нажата Кнопка - Медицинская тревога"},
    "102": {"ru": "Нажата Кнопка - Медицинская тревога"},
    "110": {"ru": "Пожарная тревога"},
    "111": {"ru": "Тревога Дымовой Детектор"},
    "112": {"ru": "Тревога Возгорание"},
    "113": {"ru": "Тревога Утечка Воды"},
    "114": {"ru": "Тревога Тепловой Детектор"},
    "115": {"ru": "Нажата Кнопка Пожар"},
    "116": {"ru": "Тревога в Трубопроводе"},
    "117": {"ru": "Тревога Детектор Пламени"},
    "118": {"ru": "Вероятная Тревога"},
    "120": {"ru": "Нажата Кнопка Паника"},
    "121": {"ru": "Тревога из-за Принуждения"},
    "122": {"ru": "Тихая Тревога"},
    "123": {"ru": "Звуковая Тревога; Кнопка"},
    "124": {"ru": "Принуждение, Вход Разрешен"},
    "125": {"ru": "Принуждение, Выход Разрешен"},
    "130": {"ru": "Тревога в зоне"},
    "131": {"ru": "Тревога в зоне периметра"},
    "132": {"ru": "Тревога в зоне внутренняя"},
    "133": {"ru": "Тревога в 24-часовой зоне"},
    "134": {"ru": "Тревога в зоне Вход/Выход"},
    "135": {"ru": "Тревога в зоне День/Ночь"},
    "136": {"ru": "Тревога в зоне Наружная"},
    "137": {"ru": "Тревога в зоне Тамперная"},
    "138": {"ru": "Вероятная Тревога"},
    "139": {"ru": "Верификатор Проникновения"},
    "140": {"ru": "Общая тревога"},
    "141": {"ru": "Петля открыта"},
    "142": {"ru": "Петля закорочена"},
    "143": {"ru": "Неисправность Модуля"},
    "144": {"ru": "Взлом Тампера детектора"},
    "145": {"ru": "Взлом Тампера модуля расширения"},
    "146": {"ru": "Тихая тревога; взлом"},
    "147": {"ru": "Неудача контроля детектора"},
    "150": {"ru": "Тревога 24-часовая; не охранная зона"},
    "151": {"ru": "Тревога; Детектор Газа"},
    "152": {"ru": "Тревога; Холодильник"},
    "153": {"ru": "Тревога; Утечка Тепла"},
    "154": {"ru": "Тревога; Протечка Воды"},
    "155": {"ru": "Тревога"},
    "156": {"ru": "Неисправность - День"},
    "157": {"ru": "Низкий уровень газа в баллоне"},
    "158": {"ru": "Высокая Температура"},
    "159": {"ru": "Низкая Температура"},
    "161": {"ru": "Уменьшение Воздушного Потока"},
    "162": {"ru": "Тревога, Угарный Газ"},
    "163": {"ru": "Неверный уровень в Резервуаре"},
    "200": {"ru": "Контроль Пожара"},
    "201": {"ru": "Низкий давление Воды"},
    "202": {"ru": "Низкая Концентрация СО2"},
    "203": {"ru": "Датчик Вентиля"},
    "204": {"ru": "Низкий уровень Воды"},
    "205": {"ru": "Насос Включен"},
    "206": {"ru": "Неисправность Насоса"},
    "320": {"ru": "Неисправность Сирены/Реле"},
    "321": {"ru": "Неисправность Сирены 1"},
    "322": {"ru": "Неисправность Сирены 2"},
    "323": {"ru": "Неисправность Реле Тревоги"},
    "324": {"ru": "Неисправность Реле Неисправность"},
    "325": {"ru": "Неисправность Реверсирование"},
    "326": {"ru": "Извещение Устройство № 3"},
    "327": {"ru": "Извещение Устройство № 4"},
    "330": {"ru": "Неисправность системной периферии"},
    "331": {"ru": "Адресный шлейф открыт"},
    "332": {"ru": "Адресный шлейф К.З."},
    "333": {"ru": "Неисправность модуля расширения"},
    "334": {"ru": "Неисправность повторителя"},
    "335": {"ru": "Принтер, нет бумаги"},
    "336": {"ru": "Потеря связи с принтером"},
    "337": {"ru": "Отсутствие DC питания внешнего модуля"},
    "338": {"ru": "Низкое напряжение аккумулятора внешнего модуля"},
    "339": {"ru": "Перезагрузка внешнего модуля"},
    "341": {"ru": "Вскрытие внешнего модуля"},
    "342": {"ru": "Отсутствие AC питания внешнего модуля"},
    "343": {"ru": "Неудача самотестирования внешнего модуля"},
    "344": {"ru": "Обнаружена Помеха на RF устройстве"},
    "350": {"ru": "Нет связи со станцией мониторинга"},
    "351": {"ru": "Неисправность телефонной линии 1"},
    "352": {"ru": "Неисправность телефонной линии 2"},
    "353": {"ru": "Неисправность передатчика дальнего действия"},
    "354": {"ru": "Отсутствие связи со станцией мониторинга"},
    "355": {"ru": "Отсутствие контроля передатчика дальнего действия"},
    "356": {"ru": "Потеря опроса с Центра"},
    "357": {"ru": "Проблема КСВ для передатчика дальнего действия"},
    "370": {"ru": "Защитный шлейф"},
    "371": {"ru": "Защитный шлейф открыт"},
    "372": {"ru": "Защитный шлейф замкнут"},
    "373": {"ru": "Неисправность пожарного шлейфа"},
    "374": {"ru": "Ошибка Выходной зоны"},
    "375": {"ru": "Неисправность зоны Паника"},
    "376": {"ru": "Неисправность зоны"},
    "377": {"ru": "Неисправность датчика наклона"},
    "378": {"ru": "Неисправность связанных зон"},
    "380": {"ru": "Неисправность Сенсора"},
    "381": {"ru": "Потеря контроля за передатчиком"},
    "382": {"ru": "Потеря контроля RPM"},
    "383": {"ru": "Неисправность Тампер Детектора"},
    "384": {"ru": "Разряжена батарейка передатчика"},
    "385": {"ru": "Детектор Дыма; высокая чувствительность"},
    "386": {"ru": "Детектор Дыма; низкая чувствительность"},
    "387": {"ru": "Детектор Охраны; высокая чувствительность"},
    "388": {"ru": "Детектор Охраны; низкая чувствительность"},
    "389": {"ru": "Ошибка самодиагностики Детектора"},
    "391": {"ru": "Ошибка контроля Детектора"},
    "392": {"ru": "Ошибка компенсации ухода частоты"},
    "393": {"ru": "Сигнал о техническом обслуживании"},
    "400": {"ru": "Снятие/Постановка с охраны"},
    "401": {"ru": "Снятие/Постановка пользователем"},
    "402": {"ru": "Снятие/Постановка раздела"},
    "403": {"ru": "Автоматическое снятие/постановка на охрану"},
    "404": {"ru": "Снятие/Постановка после установленного времени"},
    "405": {"ru": "Прерывание автоматической постановки"},
    "406": {"ru": "Отмена Тревоги"},
    "407": {"ru": "Снятие/Постановка с охраны с компьютера"},
    "408": {"ru": "Быстрое Снятие/Постановка"},
    "409": {"ru": "Снятие/Постановка с охраны переключателем"},
    "411": {"ru": "Запрос на ответный звонок"},
    "412": {"ru": "Удачный сеанс выгрузки"},
    "413": {"ru": "Неудачный сеанс выгрузки"},
    "414": {"ru": "Получена команда системного останова"},
    "415": {"ru": "Получена команда останова наборщика"},
    "416": {"ru": "Удачный сеанс загрузки"},
    "421": {"ru": "Доступ запрещен"},
    "422": {"ru": "Рапорт доступа пользователем"},
    "423": {"ru": "Доступ под принуждением"},
    "424": {"ru": "Выход Запрещен"},
    "425": {"ru": "Выход Разрешен"},
    "426": {"ru": "Дверь разблокирована и открыта"},
    "427": {"ru": "Неисправность, контроль статуса двери"},
    "428": {"ru": "Неисправность устройства"},
    "429": {"ru": "Вход в программирование доступа"},
    "430": {"ru": "Выход из программирования доступа"},
    "431": {"ru": "Изменение уровня доступа"},
    "432": {"ru": "Реле доступа не сработало"},
    "433": {"ru": "Запрос на Выход шунтирован"},
    "434": {"ru": "Контроль статуса двери шунтирован"},
    "441": {"ru": "Постановка с присутствием людей"},
    "442": {"ru": "Переключатель; Постановка с присутствием людей"},
    "450": {"ru": "Невозможность Снятия/Постановки"},
    "451": {"ru": "Снятие/Постановка до установленного времени"},
    "452": {"ru": "Снятие/Постановка после установленного времени"},
    "453": {"ru": "Отсутствие Снятия в установленное время"},
    "454": {"ru": "Отсутствие Постановки в установленное время"},
    "455": {"ru": "Неудача Автоматической Постановки"},
    "456": {"ru": "Частичная Постановка"},
    "457": {"ru": "Ошибка; Зона выхода открыта после выходной задержки"},
    "459": {"ru": "Тревога после недавней постановки пользователем"},
    "461": {"ru": "Ввод некорректного Кода"},
    "462": {"ru": "Ввод корректного Кода"},
    "463": {"ru": "Перепостановка после Тревоги"},
    "464": {"ru": "Время Автоматической Постановки увеличено"},
    "465": {"ru": "Сброс Тревоги Паника"},
    "466": {"ru": "Сервисная служба в/вне помещении"},
    "501": {"ru": "Считыватель отключен"},
    "520": {"ru": "Сирена/Реле отключена"},
    "521": {"ru": "Сирена 1 отключена"},
    "522": {"ru": "Сирена 2 отключена"},
    "523": {"ru": "Реле Тревоги отключено"},
    "524": {"ru": "Реле Неисправность отключено"},
    "525": {"ru": "Реверсирование Реле отключено"},
    "526": {"ru": "Извещение Устройство № 3 отключено"},
    "527": {"ru": "Извещение Устройство № 4 отключено"},
    "531": {"ru": "Добавлен модуль"},
    "532": {"ru": "Модуль удален"},
    "551": {"ru": "Коммуникатор отключен"},
    "552": {"ru": "Передатчик дальнего действия отключен"},
    "553": {"ru": "Удаленная Загрузка/Выгрузка отключена"},
    "570": {"ru": "Зона отключена"},
    "571": {"ru": "Пожарная зона отключена"},
    "572": {"ru": "24-часовая зона отключена"},
    "573": {"ru": "Мгновенная Зона Охраны отключена"},
    "574": {"ru": "Групповое отключение зон"},
    "575": {"ru": "Swinger отключен"},
    "576": {"ru": "Зона Доступа шунтирована"},
    "577": {"ru": "Зона Доступа отключена"},
    "601": {"ru": "Ручной тест посылки сообщений"},
    "602": {"ru": "Периодический тестовый отчет"},
    "603": {"ru": "Периодическая беспроводная передача"},
    "604": {"ru": "Пожарный тест"},
    "605": {"ru": "Отчет статуса"},
    "606": {"ru": "Голосовая связь"},
    "607": {"ru": "Режим Тест-Прохода детекторов"},
    "608": {"ru": "Периодический Тест - Существует Системная Неисправность"},
    "609": {"ru": "Видео-передача активирована"},
    "611": {"ru": "Контрольная точка пройдена"},
    "612": {"ru": "Контрольная точка не пройдена"},
    "613": {"ru": "Охранная Зона протестирована в режиме Тест-Проход"},
    "614": {"ru": "Кнопка Паники протестирована в режиме Тест-Проход"},
    "615": {"ru": "Охранная Зона протестирована в режиме Тест-Проход"},
    "616": {"ru": "Вызов Сервисной Службы"},
}


def get_description(code: str) -> str:
    info = EVENT_CODES.get(code)
    if not info:
        return ""
    if LANG in info and info[LANG]:
        return info[LANG]
    if "ru" in info:
        return info["ru"]
    return ""


def init_db():
    global DB_CONN
    if not ARCHIVE_ENABLED:
        return None
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "ts TEXT,"
            "raw TEXT,"
            "account TEXT,"
            "type TEXT,"
            "code TEXT,"
            "qual TEXT,"
            "msg_type TEXT,"
            "grp INTEGER,"
            "zone INTEGER"
            ");"
        )
        conn.commit()
        DB_CONN = conn
        return conn
    except Exception as e:
        print("[IPRO12] SQLite init error:", e)
        DB_CONN = None
        return None


def save_event_to_db(event):
    if not ARCHIVE_ENABLED or DB_CONN is None:
        return
    try:
        cur = DB_CONN.cursor()
        cur.execute(
            "INSERT INTO events (ts, raw, account, type, code, qual, msg_type, grp, zone) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (
                datetime.utcnow().isoformat(timespec="seconds"),
                event.get("raw"),
                event.get("account"),
                event.get("type"),
                event.get("code"),
                event.get("qualifier"),
                event.get("msg_type"),
                event.get("group"),
                event.get("zone"),
            ),
        )
        DB_CONN.commit()
    except Exception as e:
        print("[IPRO12] SQLite insert error:", e)


def query_events(limit=100, zone=None, etype=None):
    if DB_CONN is None:
        return []
    try:
        cur = DB_CONN.cursor()
        sql = "SELECT ts, raw, account, type, code, qual, msg_type, grp, zone FROM events"
        params = []
        cond = []
        if zone is not None:
            cond.append("zone = ?")
            params.append(zone)
        if etype is not None:
            cond.append("type = ?")
            params.append(etype)
        if cond:
            sql += " WHERE " + " AND ".join(cond)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        rows = cur.fetchall()
        res = []
        for r in rows:
            code = r[4]
            res.append(
                {
                    "ts": r[0],
                    "raw": r[1],
                    "account": r[2],
                    "type": r[3],
                    "code": code,
                    "qualifier": r[5],
                    "msg_type": r[6],
                    "group": r[7],
                    "zone": r[8],
                    "description": get_description(code),
                }
            )
        return res
    except Exception as e:
        print("[IPRO12] SQLite query error:", e)
        return []


def mqtt_publish(topic_suffix, payload, retain=False):
    if not USE_MQTT:
        return
    try:
        topic = f"{MQTT_BASE_TOPIC}/{topic_suffix}" if topic_suffix else MQTT_BASE_TOPIC
        client = mqtt.Client()
        if MQTT_USER or MQTT_PASS:
            client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.publish(topic, payload, retain=retain)
        client.disconnect()
    except Exception as e:
        print("[IPRO12] MQTT publish error:", e)


def send_webhook(data):
    if not WEBHOOK_ENABLED or not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json=data, timeout=5)
    except Exception as e:
        print("[IPRO12] Webhook error:", e)


def mqtt_discovery():
    if not USE_MQTT:
        return
    try:
        client = mqtt.Client()
        if MQTT_USER or MQTT_PASS:
            client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.connect(MQTT_HOST, MQTT_PORT, 60)

        def pub(topic, payload):
            client.publish(topic, json.dumps(payload), retain=True)

        base = DISCOVERY_PREFIX
        dev = {
            "identifiers": ["ipro12_panel"],
            "name": "IPRO-12 Panel",
            "manufacturer": "IPRO",
            "model": "IPRO-12",
        }

        alarm_cfg = {
            "name": "IPRO12 Тревога",
            "unique_id": "ipro12_alarm",
            "state_topic": f"{MQTT_BASE_TOPIC}/status/alarm",
            "device_class": "safety",
            "device": dev,
        }
        pub(f"{base}/binary_sensor/ipro12_alarm/config", alarm_cfg)

        arm_cfg = {
            "name": "IPRO12 Под охраной",
            "unique_id": "ipro12_arm",
            "state_topic": f"{MQTT_BASE_TOPIC}/status/arm",
            "device_class": "lock",
            "device": dev,
        }
        pub(f"{base}/binary_sensor/ipro12_arm/config", arm_cfg)

        conn_cfg = {
            "name": "IPRO12 Связь",
            "unique_id": "ipro12_connection",
            "state_topic": f"{MQTT_BASE_TOPIC}/status/connection",
            "icon": "mdi:access-point-network",
            "device": dev,
        }
        pub(f"{base}/sensor/ipro12_connection/config", conn_cfg)

        last_cfg = {
            "name": "IPRO12 Последнее событие",
            "unique_id": "ipro12_last_event",
            "state_topic": f"{MQTT_BASE_TOPIC}/status/last_event",
            "icon": "mdi:history",
            "device": dev,
        }
        pub(f"{base}/sensor/ipro12_last_event/config", last_cfg)

        power_cfg = {
            "name": "IPRO12 Питание",
            "unique_id": "ipro12_power",
            "state_topic": f"{MQTT_BASE_TOPIC}/status/power",
            "icon": "mdi:power-plug",
            "device": dev,
        }
        pub(f"{base}/sensor/ipro12_power/config", power_cfg)

        batt_cfg = {
            "name": "IPRO12 Аккумулятор",
            "unique_id": "ipro12_battery",
            "state_topic": f"{MQTT_BASE_TOPIC}/status/battery",
            "icon": "mdi:car-battery",
            "device": dev,
        }
        pub(f"{base}/sensor/ipro12_battery/config", batt_cfg)

        for zone in range(1, 33):
            zcfg = {
                "name": f"IPRO12 Зона {zone}",
                "unique_id": f"ipro12_zone_{zone}",
                "state_topic": f"{MQTT_BASE_TOPIC}/zone/{zone}",
                "device_class": "safety",
                "device": dev,
            }
            pub(f"{base}/binary_sensor/ipro12_zone_{zone}/config", zcfg)

        client.disconnect()
        print("[IPRO12] MQTT discovery published")
    except Exception as e:
        print("[IPRO12] MQTT discovery error:", e)


def parse_contact_id(data_raw: str):
    # STEMAX format: 5000 18AAAAQXXXYYZZZ
    s = "".join(ch for ch in data_raw.upper() if ch.isalnum())
    if "5000" in s:
        idx = s.find("5000")
        pos = idx + 4
        if pos + 2 <= len(s) and s[pos:pos+2] == "18":
            pos += 2
            acct_digits = []
            while pos < len(s) and s[pos].isdigit():
                acct_digits.append(s[pos])
                pos += 1
            if not acct_digits or pos >= len(s):
                return None
            acct = "".join(acct_digits)
            qual = s[pos]
            pos += 1
            if pos + 3 > len(s):
                return None
            code = s[pos:pos+3]
            pos += 3
            if pos + 2 > len(s):
                part = "00"
            else:
                part = s[pos:pos+2]
                pos += 2
            if pos + 3 > len(s):
                zone = "000"
            else:
                zone = s[pos:pos+3]
            msg_type = "18"
        else:
            return None
    else:
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) < 16:
            return None
        acct = digits[0:4]
        msg_type = digits[4]
        qual = digits[5]
        code = digits[6:9]
        part = digits[9:11]
        zone = digits[11:14]

    event_type = "event"
    if code in (
        "100","101","102","110","111","112","113","114","115","116","117","118",
        "120","121","122","123","124","125","130","131","132","133","134","135",
        "136","137","138","139","140","141","142","143","144","145","146","147",
        "150","151","152","153","154","155","156","157","158","159","161","162",
        "163"
    ):
        if qual == "E":
            event_type = "alarm"
        elif qual == "R":
            event_type = "alarm_restore"
    elif code == "301":
        event_type = "power_lost"
    elif code == "302":
        event_type = "power_restore"
    elif code in ("400","401","402","403","404","405","406","407","408","409","441","442"):
        if qual == "E":
            event_type = "arm_event"
        elif qual == "R":
            event_type = "arm_restore"
    elif code == "339":
        event_type = "battery_low"

    try:
        grp_int = int(part)
    except ValueError:
        grp_int = 0
    try:
        zone_int = int(zone)
    except ValueError:
        zone_int = 0

    description = get_description(code)

    return {
        "account": acct,
        "raw": s,
        "type": event_type,
        "code": code,
        "qualifier": qual,
        "msg_type": msg_type,
        "group": grp_int,
        "zone": zone_int,
        "description": description,
    }


def update_states_from_event(event):
    global LAST_EVENT_TS, CONNECTION_STATE, ARMED, ALARM_ACTIVE, POWER_STATE, BATTERY_STATE

    with STATE_LOCK:
        LAST_EVENT_TS = datetime.utcnow()
        prev_conn = CONNECTION_STATE
        CONNECTION_STATE = "online"

        ev_type = event.get("type")
        code = event.get("code")
        qual = event.get("qualifier")

        if ev_type == "alarm":
            ALARM_ACTIVE = True
        elif ev_type == "alarm_restore":
            ALARM_ACTIVE = False

        if code == "301" and qual == "E":
            POWER_STATE = "lost"
        elif code == "302" and qual == "E":
            POWER_STATE = "normal"

        if code == "339" and qual == "E":
            BATTERY_STATE = "low"

        if code in ("400","401","402","403","404","405","406","407","408","409","441","442"):
            if qual == "E":
                ARMED = True
            elif qual == "R":
                ARMED = False
                ALARM_ACTIVE = False

        if prev_conn != CONNECTION_STATE:
            mqtt_publish("status/connection", CONNECTION_STATE, retain=True)


def publish_status():
    with STATE_LOCK:
        mqtt_publish("status/arm", "on" if ARMED else "off", retain=True)
        mqtt_publish("status/alarm", "on" if ALARM_ACTIVE else "off", retain=True)
        mqtt_publish("status/power", POWER_STATE, retain=True)
        mqtt_publish("status/battery", BATTERY_STATE, retain=True)
        mqtt_publish("status/connection", CONNECTION_STATE, retain=True)


def supervision_watchdog():
    global CONNECTION_STATE
    while True:
        time.sleep(10)
        with STATE_LOCK:
            delta = datetime.utcnow() - LAST_EVENT_TS
            if delta.total_seconds() > SUPERVISION_TIMEOUT:
                if CONNECTION_STATE != "offline":
                    CONNECTION_STATE = "offline"
                    print("[IPRO12] Supervision timeout, marking as offline")
                    mqtt_publish("status/connection", CONNECTION_STATE, retain=True)


class SimpleHandler(BaseHTTPRequestHandler):
    def _send_json(self, obj, status=200):
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, html, status=200):
        data = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/history":
            qs = parse_qs(parsed.query)
            limit = int(qs.get("limit", ["100"])[0])
            zone = qs.get("zone", [None])[0]
            etype = qs.get("type", [None])[0]

            zint = None
            if zone is not None:
                try:
                    zint = int(zone)
                except ValueError:
                    zint = None

            events = query_events(limit=limit, zone=zint, etype=etype)
            return self._send_json(events)

        if parsed.path == "/codes":
            codes = []
            for c, info in sorted(EVENT_CODES.items()):
                codes.append({"code": c, "description": get_description(c)})
            return self._send_json(codes)

        if parsed.path == "/":
            events = query_events(limit=100)
            rows = ["<tr><th>Время</th><th>Тип</th><th>Код</th><th>Описание</th><th>Зона</th><th>Группа</th><th>Raw</th></tr>"]
            for e in events:
                rows.append(
                    "<tr>"
                    f"<td>{e['ts']}</td>"
                    f"<td>{e['type']}</td>"
                    f"<td>{e['code']}</td>"
                    f"<td>{e.get('description','')}</td>"
                    f"<td>{e['zone']}</td>"
                    f"<td>{e['group']}</td>"
                    f"<td>{e['raw']}</td>"
                    "</tr>"
                )
            table = "<table border='1' cellspacing='0' cellpadding='4'>" + "".join(rows) + "</table>"
            html = (
                "<!DOCTYPE html><html><head><meta charset='utf-8'>"
                "<title>IPRO12 Surgard History</title></head><body>"
                "<h1>IPRO12 Surgard Receiver</h1>"
                "<p>Последние события (макс 100):</p>"
                + table +
                "</body></html>"
            )
            return self._send_html(html)

        self.send_error(404, "Not Found")


def start_http_server():
    server = HTTPServer(("0.0.0.0", HTTP_PORT), SimpleHandler)
    print(f"[IPRO12] HTTP server started on port {HTTP_PORT}")
    server.serve_forever()


def start_surgard_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", SURGARD_PORT))
    s.listen(5)

    print(f"[IPRO12] Surgard Receiver started on port {SURGARD_PORT}")
    print(f"[IPRO12] MQTT enabled: {USE_MQTT}, host: {MQTT_HOST}:{MQTT_PORT}, base: {MQTT_BASE_TOPIC}")
    print(f"[IPRO12] Webhook enabled: {WEBHOOK_ENABLED}, url: {WEBHOOK_URL}")
    print(f"[IPRO12] Archive enabled: {ARCHIVE_ENABLED}, db: {DB_PATH}")
    print(f"[IPRO12] Supervision timeout: {SUPERVISION_TIMEOUT} sec")
    print(f"[IPRO12] Language: {LANG}")

    if USE_MQTT:
        mqtt_discovery()

    while True:
        conn, addr = s.accept()
        try:
            data = conn.recv(1024).decode(errors="ignore")
            if not data:
                conn.close()
                continue

            print(f"[IPRO12] RAW from {addr}: {repr(data)}")
            event = parse_contact_id(data)
            if event:
                print("[IPRO12] Parsed event:", event)
                save_event_to_db(event)
                update_states_from_event(event)
                mqtt_publish("event", json.dumps(event, ensure_ascii=False), retain=False)
                mqtt_publish(f"zone/{event['zone']}", event["type"], retain=False)
                mqtt_publish(
                    "status/last_event",
                    f"{event['type']} code {event['code']} zone {event['zone']}",
                    retain=True,
                )
                publish_status()
                send_webhook(event)

            try:
                conn.sendall(b"\x06")
            except Exception:
                pass

        except Exception as e:
            print("[IPRO12] Error handling connection:", e)
        finally:
            conn.close()


if __name__ == "__main__":
    init_db()

    t_http = threading.Thread(target=start_http_server, daemon=True)
    t_http.start()

    t_sup = threading.Thread(target=supervision_watchdog, daemon=True)
    t_sup.start()

    start_surgard_server()
