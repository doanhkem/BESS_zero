from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Endian
import datetime
import time
import sys
import logging
from logging.handlers import RotatingFileHandler
import struct
import os

import paho.mqtt.client as mqtt
import json
import threading

# import keyboard


sys.stdout.reconfigure(encoding="utf-8")

# ⚡ Cấu hình thiết bị
# DATA_MANAGEMENT_IP = "192.168.1.22"
# DATA_MANAGEMENT_ID = 2
# LOAD_METER_ID = 12


DATA_MANAGEMENT_IP = "192.168.1.2"
DATA_MANAGEMENT_ID = 3
LOAD_METER_ID = 13

PVmax = 125000
pcs_gain = 1

LOAD_CONSUMPTION_REG = 30865
LOAD_CONSUMPTION_REG_2 = 30867
TOTAL_INVERTER_POWER_REG = 30775
MAX_POWER_REG = 41463  # Thanh ghi 32-bit

BESS_IP = "192.168.1.100"
BESS_ID = 1
BESS_POWER_REG = 570
BESS_SOC_REG = 587
BESS_CHARGE_POWER_REG = 618

MODBUS_TCP_PORT = 502


def decode_faults(register_values):

    fault_definitions = [
        # Thanh ghi 1
        "Phase-lock alarm",
        "DC side hardware soft start fault",
        "AC side hardware soft start fault",
        "DC software soft start fault",
        "Grid resonance fault",
        "Input impedance fault",
        "Input impedance alarm",
        "CANB communication abnormal alarm",
        "The system has address conflict fault",
        "System master has address conflict fault",
        "Module address abnormal fault",
        "DC voltage sampling fault alarm",
        "AC voltage sampling abnormal alarm",
        "DC side main relay alarm",
        "Busbar midpoint uneven alarm",
        "Overload alarm",
        # Thanh ghi 2
        "Positive bus secondary overvoltage alarm",
        "Negative bus secondary overvoltage alarm",
        "Module A1 phase overcurrent alarm",
        "Module B1 phase overcurrent alarm",
        "Module C1 phase overcurrent alarm",
        "Module A2 phase overcurrent alarm",
        "Module B2 phase overcurrent alarm",
        "Module C2 phase overcurrent alarm",
        "Auxiliary power supply fault",
        "Parallel CANA communication fault",
        "Fan 1 fault",
        "Fan 2 fault",
        "Fan 3 fault",
        "Inverter overvoltage alarm",
        "Inverter undervoltage alarm",
        "Emergency stop fault",
        # Thanh ghi 3
        "RS485 communication fault",
        "AC current sampling fault",
        "AC output short circuit fault",
        "Inverse amplitude lockout alarm",
        "Inverter phase sequence alarm",
        "Grid phase sequence alarm",
        "DC port undervoltage alarm",
        "DC port overvoltage alarm",
        "Negative bus level 1 overvoltage alarm",
        "Positive bus level 1 overvoltage alarm",
        "Low busbar voltage alarm",
        "High busbar voltage alarm",
        "Grid frequency low alarm",
        "Grid frequency high alarm",
        "Low grid voltage alarm",
        "High grid voltage alarm",
        # Thanh ghi 4
        "High ambient temperature alarm at air inlet",
        "High ambient temperature alarm at air outlet",
        "Module A2 high temperature alarm",
        "Module B2 high temperature alarm",
        "Module C2 high temperature alarm",
        "Module A1 high temperature alarm",
        "Module B1 high temperature alarm",
        "Module C1 high temperature alarm",
        "Module A2 temperature sensor fault",
        "Module B2 temperature sensor fault",
        "Module C2 temperature sensor fault",
        "Module A1 temperature sensor fault",
        "Module B1 temperature sensor fault",
        "Module C1 temperature sensor fault",
        "Grid access fault in AC constant voltage mode",
        "DSP software version mismatch fault",
    ]

    active_faults = []

    # Duyệt qua 4 thanh ghi
    for reg_index, reg_value in enumerate(register_values):
        for bit in range(16):  # Mỗi thanh ghi có 16 bit
            if reg_value & (1 << bit):  # Kiểm tra bit có đang bật không
                fault_index = reg_index * 16 + bit
                if fault_index < len(fault_definitions):
                    active_faults.append(fault_definitions[fault_index])

    return active_faults


script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, "logger.txt")
logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(
    log_file, maxBytes=1024 * 1024 * 20, backupCount=3, encoding="utf-8"
)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


# Địa chỉ file lưu trữ dữ liệu
data_file_path = os.path.join(os.path.dirname(__file__), "time_conf.txt")

# Biến lưu trữ dữ liệu nhận được
default_discharge_data = [
    {
        "DISCHARGE_START_H": 10,
        "DISCHARGE_START_M": 0,
        "DISCHARGE_END_H": 1,
        "DISCHARGE_END_M": 0,
        "TIMESTAMP": "2025-03-06 14:30:45",
    }
]

discharge_data = []


def load_discharge_data_from_file():
    global discharge_data
    if os.path.exists(data_file_path):
        try:
            with open(data_file_path, "r") as file:
                discharge_data = json.load(file)
                if not isinstance(discharge_data, list) or not discharge_data:
                    discharge_data = default_discharge_data.copy()
            print(discharge_data)

        except (json.JSONDecodeError, FileNotFoundError):
            discharge_data = default_discharge_data.copy()
    else:
        discharge_data = default_discharge_data.copy()
        print(discharge_data)


def save_discharge_data_to_file():
    with open(data_file_path, "w") as file:
        json.dump(discharge_data, file, indent=4)


has_responded = False


def on_message(client, userdata, msg):
    global discharge_data, has_responded
    print(f"Received message: {msg.topic} -> {msg.payload.decode()}")

    try:
        received_payload = json.loads(msg.payload.decode())
        if isinstance(received_payload, list) and received_payload:
            discharge_data = received_payload
            discharge_data[0]["TIMESTAMP"] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            has_responded = False  # Cho phép phản hồi lại khi có dữ liệu mới
            save_discharge_data_to_file()
    except json.JSONDecodeError:
        print("Invalid JSON received")

    if not has_responded:
        response_topic = msg.topic.replace(
            "CONFIG", "HEALTHCHECK", 1
        )  # Chuyển phản hồi sang topic HEALTHCHECK
        client.publish(response_topic, json.dumps(discharge_data))
        has_responded = True


def send_healthcheck(client, topic):
    while True:
        discharge_data[0]["TIMESTAMP"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        client.publish(topic, json.dumps(discharge_data))
        threading.Event().wait(10)  # Chờ 2 phút trước khi gửi lại


def mqtt_handler(
    broker: str,
    port: int,
    topic: str,
    username: str,
    password: str,
    client_id: str = "mqtt_client",
):
    client = mqtt.Client(client_id)
    client.username_pw_set(username, password)
    client.on_message = on_message

    client.connect(broker, port, 60)
    client.subscribe(topic)

    # Bắt đầu gửi healthcheck định kỳ mỗi 2 phút
    healthcheck_topic = topic.replace("CONFIG", "HEALTHCHECK", 1)
    threading.Thread(
        target=send_healthcheck, args=(client, healthcheck_topic), daemon=True
    ).start()

    client.loop_forever()


def value_decode(registers, typeString, size):
    decoder = BinaryPayloadDecoder.fromRegisters(
        registers, byteorder=Endian.Big, wordorder=Endian.Big
    )
    if typeString == "int16":
        value = decoder.decode_16bit_int()
    elif typeString == "uint16":
        value = decoder.decode_16bit_uint()
    elif typeString == "int32":
        value = decoder.decode_32bit_int()
    elif typeString == "uint32":
        value = decoder.decode_32bit_uint()
    elif typeString == "float16":
        value = decoder.decode_16bit_float()
    elif typeString == "float32":
        value = decoder.decode_32bit_float()
    elif typeString == "string":
        value = decoder.decode_string(size).decode()
    else:
        value = "Invalid type"

    return value


def is_within_timer():

    now = datetime.datetime.now()
    current_minutes = now.hour * 60 + now.minute  # Tổng số phút từ 00:00
    start_minutes = (
        discharge_data[0]["DISCHARGE_START_H"] * 60
        + discharge_data[0]["DISCHARGE_START_M"]
    )
    end_minutes = (
        discharge_data[0]["DISCHARGE_END_H"] * 60 + discharge_data[0]["DISCHARGE_END_M"]
    )
    # print(f"Thời gian hiện tại: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    # print(discharge_data)
    if start_minutes <= end_minutes:
        return start_minutes <= current_minutes < end_minutes
    else:  # Trường hợp qua đêm
        return current_minutes >= start_minutes or current_minutes < end_minutes


# 🔄 Hàm kết nối Modbus, tự retry nếu lỗi
def connect_modbus_device(ip, retries=3, delay=2):
    for _ in range(retries):
        client = ModbusTcpClient(ip, port=MODBUS_TCP_PORT)
        if client.connect():
            return client
        print(f"⚠️ Không thể kết nối {ip}, thử lại sau {delay}s...")
        logger.error(f"⚠️ Không thể kết nối {ip}, thử lại sau {delay}s...")
        time.sleep(delay)
    return None


# 📥 Đọc thanh ghi Modbus
def read_register(client, register, unit_id, type, count):
    try:
        result = client.read_holding_registers(register, count, unit=unit_id)
        if count == 4:
            return result.registers
        else:
            return value_decode(result.registers, type, count)

    except Exception as e:
        print(f"❌ Lỗi khi đọc thanh ghi {register}: {e}")
        logger.error(f"❌ Lỗi khi đọc thanh ghi {register}: {e}")
    return None


def write_register(
    client,
    register,
    value,
    unit_id,
    data_type="int32",
    byteorder=Endian.Big,
    wordorder=Endian.Big,
):
    try:
        builder = BinaryPayloadBuilder(byteorder=byteorder, wordorder=wordorder)

        # Xử lý theo kiểu dữ liệu
        if data_type == "int16":
            builder.add_16bit_int(value)
        elif data_type == "uint16":
            builder.add_16bit_uint(value)
        elif data_type == "int32":
            builder.add_32bit_int(value)
        elif data_type == "uint32":
            builder.add_32bit_uint(value)
        elif data_type == "float32":
            builder.add_32bit_float(value)
        else:
            raise ValueError(f"❌ Kiểu dữ liệu {data_type} không được hỗ trợ")

        payload = builder.to_registers()  # Chuyển thành danh sách thanh ghi
        result = client.write_registers(register, payload, unit=unit_id)

        if result and not result.isError():
            print(
                f"✅ Ghi thành công giá trị {value} ({data_type}) vào thanh ghi {register}"
            )
            logger.info(
                f"✅ Ghi thành công giá trị {value} ({data_type}) vào thanh ghi {register}"
            )
        else:
            print(f"❌ Lỗi khi ghi giá trị {value} vào thanh ghi {register}")
            logger.error(f"❌ Lỗi khi ghi giá trị {value} vào thanh ghi {register}")

    except Exception as e:
        print(f"❌ Exception khi ghi {data_type} vào thanh ghi {register}: {e}")
        logger.error(f"❌ Exception khi ghi {data_type} vào thanh ghi {register}: {e}")
    return None


# 🔄 Đọc dữ liệu BESS
def read_bess_data():
    bess_client = connect_modbus_device(BESS_IP)
    if not bess_client:
        return None, None

    bess_power = read_register(
        bess_client,
        BESS_POWER_REG,
        BESS_ID,
        "int16",
        1,
    )
    bess_soc = read_register(
        bess_client,
        BESS_SOC_REG,
        BESS_ID,
        "uint16",
        1,
    )

    bess_state = read_register(
        bess_client,
        25134,
        BESS_ID,
        "uint16",
        1,
    )

    bess_soc = bess_soc / 10
    # bess_client.close()
    return bess_power, bess_soc, bess_state


def zero_bess():
    while True:
        # if keyboard.is_pressed("q"):
        #     print("\nChương trình dừng lại!")
        #     break
        try:

            bess_client = connect_modbus_device(BESS_IP)
            data_management_client = connect_modbus_device(DATA_MANAGEMENT_IP)
            bess_power, bess_soc, PCS_state = read_bess_data()

            faults_word = read_register(bess_client, 25132, BESS_ID, "uint16", 4)
            if decode_faults(faults_word):
                bess_faults = True
            else:
                bess_faults = False

            total_solar_production = read_register(
                data_management_client,
                TOTAL_INVERTER_POWER_REG,
                DATA_MANAGEMENT_ID,
                "int32",
                2,
            )
            load_1 = read_register(
                data_management_client, LOAD_CONSUMPTION_REG, LOAD_METER_ID, "uint32", 2
            )
            time.sleep(0.1)
            load_2 = read_register(
                data_management_client,
                LOAD_CONSUMPTION_REG_2,
                LOAD_METER_ID,
                "uint32",
                2,
            )
            if is_within_timer() and total_solar_production > 0 and enb_inv == True:
                write_register(
                    data_management_client,
                    MAX_POWER_REG,
                    value=0,
                    unit_id=DATA_MANAGEMENT_ID,
                    data_type="uint32",
                )
                time.sleep(5)
                enb_inv = False
                print("🔌 Đã tắt inverter. Đang xả BESS.")
                logger.info("🔌 Đã tắt inverter. Đang xả BESS.")
                continue

            elif (
                not is_within_timer()
                and total_solar_production < 5
                and enb_inv == False
            ) or (bess_soc <= 10 and enb_inv == False):
                write_register(
                    data_management_client,
                    MAX_POWER_REG,
                    value=PVmax,
                    unit_id=DATA_MANAGEMENT_ID,
                    data_type="uint32",
                )
                write_register(
                    bess_client,
                    register=BESS_CHARGE_POWER_REG,
                    value=0,
                    unit_id=1,
                    data_type="int16",
                )

                enb_inv = True
                print(
                    "🔌 Hết thời gian xả - Đã tắt xả BESS. Bật tối đa công xuất inverter."
                )
                logger.info(
                    "🔌 Hết thời gian xả - Đã tắt xả BESS. Bật tối đa công xuất inverter."
                )
            if not data_management_client:
                print("❌ Không thể kết nối với Data Management. Dừng chương trình.")
                logger.error(
                    "❌ Không thể kết nối với Data Management. Dừng chương trình."
                )
                break

            # ☀️ Đọc tổng công suất inverter
            total_solar_production = read_register(
                data_management_client,
                TOTAL_INVERTER_POWER_REG,
                DATA_MANAGEMENT_ID,
                "int32",
                2,
            )
            if total_solar_production <= 0:
                total_solar_production = 0

            # ❌ Nếu có lỗi khi đọc, bỏ qua vòng lặp này
            if None in [load_1, load_2, total_solar_production, bess_power, bess_soc]:
                print("⚠️ Dữ liệu thiếu, bỏ qua vòng lặp.")
                logger.warning("⚠️ Dữ liệu thiếu, bỏ qua vòng lặp.")
                time.sleep(0.1)
                continue

            grid_power = (load_1 - load_2) / 1000
            total_solar_production = total_solar_production / 1000

            print(
                f"⚡ Grid: {grid_power} kW, ☀️ Solar: {total_solar_production} kW, "
                f"🔋 BESS Power: {bess_power/10} kW, SOC: {bess_soc}% "
                f"🏠 Load : {grid_power+total_solar_production+round(bess_power*0.1,2)} kW"
                f"🔋 BESS State: {decode_faults(faults_word)}"
            )
            logger.info(
                f"⚡ Grid: {grid_power} kW, ☀️ Solar: {total_solar_production} kW, "
                f"🔋 BESS Power: {bess_power/10} kW, SOC: {bess_soc}% "
                f"🏠 Load : {grid_power+total_solar_production+round(bess_power*0.1,2)} kW"
                f"🔋 BESS State: {decode_faults(faults_word)}"
            )

            if grid_power > -0.1 and grid_power < 0.2:
                print("✅ Hệ thống chạy ổn định")
                logger.info("✅ Hệ thống chạy ổn định")

            elif grid_power > 0:

                deficit = grid_power

                if is_within_timer() and bess_soc > 10 and total_solar_production <= 10:

                    # trong giờ xả ít , tăng công suất xả, solar < 10
                    print("AAAAAAAAAAAaa")
                    discharge_power = abs(grid_power * 10 + bess_power)

                    if bess_client:
                        write_register(
                            bess_client,
                            register=BESS_CHARGE_POWER_REG,
                            value=min(round(discharge_power) * pcs_gain, 1200),
                            unit_id=1,
                            data_type="int16",
                        )
                    print(
                        f"🔌 Điều chỉnh tăng công suất xả BESS: {min(round(discharge_power)/10,1200)} kW."
                    )
                    logger.info(
                        f"🔌 Điều chỉnh tăng công suất xả BESS: {min(round(discharge_power)/10,1200)} kW."
                    )
                elif (
                    bess_power < -0.5
                    and bess_soc < 100
                    and total_solar_production > 5
                    and not is_within_timer()
                    and bess_faults == False
                ):
                    # solar cấp ko đủ ,đang sạc bằng lưới -> giảm công suất
                    print("BBBBBBBBBBBB")
                    write_register(
                        bess_client,
                        register=BESS_CHARGE_POWER_REG,
                        value=max(
                            round(grid_power * 10 + bess_power / 1) * pcs_gain, -1200
                        ),
                        unit_id=1,
                        data_type="int16",
                    )
                    print(
                        f"🔌 Điều chỉnh giảm công suất sạc BESS: {max(round((grid_power*10 + bess_power/1))/10,-1200)} kW."
                    )
                    logger.info(
                        f"🔌 Điều chỉnh giảm công suất sạc BESS: {max(round((grid_power*10 + bess_power/1))/10,-1200)} kW."
                    )

                elif not is_within_timer() and bess_soc >= 100:
                    # bess đầy, lấy lưới dùng -> tăng solar
                    print("CCCCCCCCCCCC")
                    write_register(
                        data_management_client,
                        MAX_POWER_REG,
                        value=min(
                            round((deficit + total_solar_production) * 1000), PVmax
                        ),
                        unit_id=DATA_MANAGEMENT_ID,
                        data_type="uint32",
                    )
                    print(
                        f"📌 Thiếu công suất . Tăng công suất inverter {deficit + total_solar_production} kW"
                    )
                    logger.info(
                        f"📌 Thiếu công suất . Tăng công suất inverter {deficit + total_solar_production} kW"
                    )
                    write_register(
                        bess_client,
                        register=BESS_CHARGE_POWER_REG,
                        value=0,
                        unit_id=1,
                        data_type="int16",
                    )
                    print(f"🔌 Bess đã đầy. Điều chỉnh công suất sạc BESS: {0} kW.")
                    logger.info(
                        f"🔌 Bess đã đầy. Điều chỉnh công suất sạc BESS: {0} kW."
                    )

                else:

                    if is_within_timer():
                        write_register(
                            bess_client,
                            register=BESS_CHARGE_POWER_REG,
                            value=min(
                                round(grid_power * 10 + bess_power) * pcs_gain, 1200
                            ),
                            unit_id=1,
                            data_type="int16",
                        )
                        print(
                            f"🔌 Đến giờ xả. Bắt đầu xả BESS: {min(round(abs(grid_power*10+ bess_power)/10),1200)} kW."
                        )
                        logger.info(
                            f"🔌 Đến giờ xả. Bắt đầu xả BESS: {min(round(abs(grid_power*10+ bess_power)/10),1200)} kW."
                        )

                    elif bess_soc < 100 and bess_faults == False:

                        write_register(
                            data_management_client,
                            MAX_POWER_REG,
                            value=PVmax,
                            unit_id=DATA_MANAGEMENT_ID,
                            data_type="uint32",
                        )

                        write_register(
                            data_management_client,
                            40016,
                            value=100,
                            unit_id=DATA_MANAGEMENT_ID,
                            data_type="int16",
                        )

                        print(f"📌 Thiếu công suất . Tăng công suất inverter {150} kW")
                        logger.info(
                            f"📌 Thiếu công suất . Tăng công suất inverter {150} kW"
                        )

                    else:

                        write_register(
                            data_management_client,
                            MAX_POWER_REG,
                            value=min(
                                round((deficit + total_solar_production) * 1000), PVmax
                            ),
                            unit_id=DATA_MANAGEMENT_ID,
                            data_type="uint32",
                        )

                        print(
                            f"📌 Thiếu công suất . Tăng công suất inverter {grid_power+total_solar_production} kW"
                        )
                        logger.info(
                            f"📌 Thiếu công suất . Tăng công suất inverter {grid_power+total_solar_production} kW"
                        )
            elif grid_power < 0:

                excess_energy = abs(grid_power)

                if (
                    bess_soc < 100
                    and total_solar_production >= 0
                    and not is_within_timer()
                    and bess_faults == False
                ):

                    # Solar dư đang dư
                    if bess_power > 3:
                        write_register(
                            bess_client,
                            register=BESS_CHARGE_POWER_REG,
                            value=0,
                            unit_id=1,
                            data_type="int16",
                        )
                        print(f"🔌 Hết thời gian xả BESS. Chuyển mode standby: {0} kW.")
                        logger.info(
                            f"🔌 Hết thời gian xả BESS. Chuyển mode standby: {0} kW."
                        )
                        time.sleep(5)
                        continue

                    if (
                        bess_client and total_solar_production >= 5
                    ):  # xem lại vòng lặp có cần hay ko

                        write_register(
                            bess_client,
                            register=BESS_CHARGE_POWER_REG,
                            value=max(
                                round(-excess_energy * 10 + bess_power * 1) * pcs_gain,
                                -1200,
                            ),
                            unit_id=1,
                            data_type="int16",
                        )
                        print(
                            f"🔌 Solar dư - Điều chỉnh tăng công suất sạc BESS: {max(round((-excess_energy * 10 + bess_power * 1))/10,-1200)} kW."
                        )
                        logger.info(
                            f"🔌 Solar dư - Điều chỉnh tăng công suất sạc BESS: {max(round((-excess_energy * 10 + bess_power * 1))/10,-1200)} kW."
                        )

                elif (
                    bess_soc >= 100 and total_solar_production > 0
                ) or bess_faults == True:
                    # bess đầy, giảm công suất inverter

                    write_register(
                        data_management_client,
                        MAX_POWER_REG,
                        value=max(
                            round((abs(total_solar_production - excess_energy)) * 1000),
                            0,
                        ),
                        unit_id=DATA_MANAGEMENT_ID,
                        data_type="uint32",
                    )

                    print(
                        f"📌 Công suất dư thừa, giảm công suất inverter {max(round((abs(total_solar_production - excess_energy)) * 1000), 0)}"
                    )
                    logger.info(
                        f"📌 Công suất dư thừa, giảm công suất inverter {max(round((abs(total_solar_production - excess_energy)) * 1000), 0)}"
                    )
                elif bess_power > 0 and bess_soc >= 10:
                    # bess đang xả, giảm công suất xả
                    write_register(
                        bess_client,
                        register=BESS_CHARGE_POWER_REG,
                        value=min(
                            round(abs(grid_power * 10 + bess_power)) * pcs_gain, 1200
                        ),
                        unit_id=1,
                        data_type="int16",
                    )
                    print(
                        f"🔌 Điều chỉnh công suất xả BESS: {min(round(abs(grid_power*10 + bess_power))/10,1200)} kW."
                    )
                    logger.info(
                        f"🔌 Điều chỉnh công suất xả BESS: {min(round(abs(grid_power*10 + bess_power))/10,1200)} kW."
                    )
                else:
                    print("Lưới < 0, sai hết")

            time.sleep(5)
            data_management_client.close()
            bess_client.close()
            print("🔄 Chờ 5 giây để cập nhật dữ liệu mới.")
            logger.info("🔄 Chờ 5 giây để cập nhật dữ liệu mới.")

        except:
            print("🛑 Có lỗi xảy ra trong vòng lập. thực hiện vòng lập khác.")
            logger.error("🛑 Có lỗi xảy ra trong vòng lập. thực hiện vòng lập khác.")
            time.sleep(0.3)
            continue

        finally:
            if data_management_client:
                data_management_client.close()
            time.sleep(0.3)
            continue


if __name__ == "__main__":

    load_discharge_data_from_file()
    print("🚀 Bắt đầu quản lý năng lượng...")
    logger.info("🚀 Bắt đầu quản lý năng lượng...")

    data_management_client = connect_modbus_device(DATA_MANAGEMENT_IP)
    if not data_management_client:
        print("❌ Không thể kết nối với Data Management. Dừng chương trình.")
        logger.error("❌ Không thể kết nối với Data Management. Dừng chương trình.")

    write_register(
        data_management_client,
        MAX_POWER_REG,
        value=PVmax,
        unit_id=DATA_MANAGEMENT_ID,
        data_type="uint32",
    )
    time.sleep(5)
    enb_inv = True
    threading.Thread(
        target=mqtt_handler,
        args=(
            "core.ziot.vn",
            5000,
            "CONFIG/DO000000/PO000012/SI0000014/PL000027/dischargeConfig",
            "iot2022",
            "iot2022",
        ),
    ).start()
    threading.Thread(target=zero_bess).start()
