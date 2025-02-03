# import asyncio
# import logging
from pymodbus.client.sync import ModbusTcpClient  # Chỉnh sửa lại import tại đây
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian
import datetime
import time
import sys
import io

# Đặt mã hóa của đầu ra console là utf-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# from pymodbus.client import ModbusTcpClient
# import datetime
# import time

# Giới hạn dung lượng pin và thời gian xả
DISCHARGE_START = 18  # Giờ bắt đầu xả
DISCHARGE_END = 5  # Giờ kết thúc xả

# Danh sách IP của các inverter
INVERTER_IPS = [
    "127.0.0.100",
    "127.0.0.101",
]  # Thêm IP inverter tại đây
BESS_IP = "127.0.0.103"  # IP của BESS
MODBUS_TCP_PORT = 502

# Cấu hình đồng hồ đo tải (Modbus TCP)
LOAD_METER_IP = "127.0.0.104"  # Thay bằng IP của đồng hồ đo tải Modbus TCP
LOAD_METER_PORT = 502  # Port Modbus TCP
LOAD_METER_UNIT_ID = 1  # Unit ID của đồng hồ đo tải
LOAD_CONSUMPTION_REG = 100  # Thanh ghi đo tải (kW)

# Thanh ghi Modbus của Inverter
SOLAR_POWER_REG = 100  # Công suất hiện tại của inverter (kW)
INVERTER_POWER_CMD = 101  # Lệnh giới hạn công suất cho inverter (kW)

# BESS
BESS_POWER_REG = 102  # Công suất hiện tại của BESS (kW)
BESS_SOC_REG = 103  # SOC (state of charge) hiện tại của BESS (%)
BESS_DISCHARGE_CMD = 104  # Lệnh xả BESS (0: Tắt, 1: Bật)
BESS_CHARGE_CMD = 105  # Lệnh sạc BESS (0: Tắt, 1: Bật)
BESS_CHARGE_POWER_REG = 106  # Công suất sạc tối đa vào BESS (kW)


def value_decode(registers, typeString, size, byte_order, word_order, gain):
    decoder = BinaryPayloadDecoder.fromRegisters(
        registers, byteorder=byte_order, wordorder=word_order
    )
    value = None
    try:
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
        return value / 10**gain
    except:
        print("decode error")
        return None


# Hàm kiểm tra thời gian xả
def is_within_timer(start_hour, end_hour):
    now = datetime.datetime.now()
    return start_hour <= now.hour or now.hour < end_hour


# Hàm đọc giá trị từ Modbus
def read_register(client, register, count=1):
    try:
        result = client.read_holding_registers(register, count, unit=LOAD_METER_UNIT_ID)
        if result.isError():
            print(f"Không the đọc thanh ghi {register}")
            return None
        return result
    except Exception as e:
        print(f"Loi khi đọc thanh ghi {register}: {e}")
        return None


# Hàm ghi giá trị vào Modbus
def write_register(client, register, value):
    try:
        value = round(value)
        result = client.write_register(register, value, unit=LOAD_METER_UNIT_ID)
        if result.isError():
            print(f"Không the ghi vào thanh ghi {register}")
        else:
            print(f"Ghi thành công {value} vào thanh ghi {register}")
    except Exception as e:
        print(f"LOi khi ghi vào thanh ghi {register}: {e}")


# Hàm kết nối với inverter hoặc BESS qua TCP
def connect_modbus_device(ip):
    client = ModbusTcpClient(ip, port=MODBUS_TCP_PORT)
    if not client.connect():
        print(f"KHONG THE KET NOI {ip}.")
        return None
    return client


# Hàm đọc công suất và SOC từ BESS
def read_bess_data():
    bess_client = connect_modbus_device(BESS_IP)
    if not bess_client:
        return None, None, None

    bess_power = read_register(bess_client, BESS_POWER_REG)
    bess_power = value_decode(
        bess_power.registers, "int16", 1, Endian.Big, Endian.Big, gain=0
    )
    bess_soc = read_register(bess_client, BESS_SOC_REG)
    bess_soc = value_decode(
        bess_soc.registers, "int16", 1, Endian.Big, Endian.Big, gain=0
    )
    bess_charge_power = read_register(bess_client, BESS_CHARGE_POWER_REG)
    bess_charge_power = value_decode(
        bess_charge_power.registers, "int16", 1, Endian.Big, Endian.Big, gain=0
    )
    bess_client.close()

    return bess_power, bess_soc, bess_charge_power


# Khởi động quản lý năng lượng
print("BAT DAU QUAN LY NANG LUONG...")
# load_meter_client = connect_modbus_device(LOAD_METER_IP)

# if not load_meter_client:
#     exit("Khong the ket noi voi dong ho do tai qua Modbus TCP.")




try:
    while True:
        load_meter_client = connect_modbus_device(LOAD_METER_IP)

        if not load_meter_client:
            exit("Khong the ket noi voi dong ho do tai qua Modbus TCP.")

        kac = read_register(load_meter_client, LOAD_CONSUMPTION_REG)
        grid_power = value_decode(
            kac.registers, "int16", 1, Endian.Big, Endian.Big, gain=0
        )
        if grid_power is None:
            time.sleep(5)
            continue

        total_solar_production = 0
        working_inverters = []

        for ip in INVERTER_IPS:
            inverter_client = connect_modbus_device(ip)
            if inverter_client:
                solar_power = read_register(inverter_client, SOLAR_POWER_REG)
                solar_power = value_decode(
                    solar_power.registers, "int16", 1, Endian.Big, Endian.Big, gain=0
                )
                if solar_power is not None:
                    total_solar_production += solar_power
                    working_inverters.append(inverter_client)
                else:
                    inverter_client.close()

        bess_power, bess_soc, bess_charge_power = read_bess_data()
        if bess_power is None or bess_soc is None or bess_charge_power is None:
            time.sleep(5)
            continue

        print(
            f"Grid: {grid_power} kW, Solar: {total_solar_production} kW, "
            f"BESS Power: {bess_power} kW, SOC: {bess_soc}%, Charge Power: {bess_charge_power} kW"
        )

        if grid_power > 0:
            # Nhà máy đang lấy điện từ lưới
            # deficit = grid_power
            # inverter_max_power = 100  # Công suất tối đa mỗi inverter (giả sử)

            # total_inverter_power = 0
            # for ip in INVERTER_IPS:
            #     inverter_client = connect_modbus_device(ip)
            #     if inverter_client:
            #         current_power = read_register(inverter_client, SOLAR_POWER_REG)
            #         if current_power is not None:
            #             total_inverter_power += current_power

            #             # Nếu inverter chưa chạy hết công suất tối đa, điều chỉnh tăng công suất
            #             if current_power < inverter_max_power:
            #                 additional_power = min(deficit, inverter_max_power - current_power)
            #                 write_register(inverter_client, INVERTER_POWER_CMD, (current_power + additional_power))
            #                 deficit -= additional_power

            #         inverter_client.close()

            deficit = grid_power
            inverter_max_power = 100  # Công suất tối đa mỗi inverter (giả sử)
            current_power = []

            total_inverter_power = 0
            for ip in INVERTER_IPS:
                inverter_client = connect_modbus_device(ip)
                if inverter_client:
                    resutl = read_register(inverter_client, SOLAR_POWER_REG)
                    resutl = value_decode(resutl.registers, "int16", 1, Endian.Big, Endian.Big, gain=0)
                    current_power.append(resutl)
            if current_power is not None:
                total_inverter_power = sum(current_power)

                # Nếu inverter chưa chạy hết công suất tối đa, điều chỉnh tăng công suất

            for i in range(len(INVERTER_IPS)):
                # if current_power[i] < inverter_max_power:
                # additional_power = min(deficit, inverter_max_power - current_power)
                write_register(
                    inverter_client,
                    INVERTER_POWER_CMD,
                    min(
                        (round(total_inverter_power + grid_power) / len(INVERTER_IPS)),
                        inverter_max_power,
                    ),
                )
                inverter_client.close()
            deficit = round(
                grid_power+ total_inverter_power
                - min(
                    (round(total_inverter_power + grid_power) / len(INVERTER_IPS)),inverter_max_power,
                )
                * len(INVERTER_IPS)
            )

            # Nếu vẫn còn thiếu hụt sau khi tăng công suất inverter, dùng BESS để bù
            if (
                deficit > 0
                and is_within_timer(DISCHARGE_START, DISCHARGE_END)
                and bess_soc > 10
            ):
                discharge_power = min(deficit, bess_power)
                bess_client = connect_modbus_device(BESS_IP)
                if bess_client:
                    write_register(bess_client, BESS_DISCHARGE_CMD, 1)
                    write_register(bess_client, BESS_POWER_REG, discharge_power)
                    bess_client.close()
            else:
                print("Bo sung tai bang luoi dien.")

        elif grid_power < 0:
            # Nhà máy đang xả điện ra lưới
            excess_energy = abs(grid_power)
            if bess_soc < 90 and excess_energy <= bess_charge_power:
                charge_power = excess_energy
                bess_client = connect_modbus_device(BESS_IP)
                if bess_client:
                    write_register(bess_client, BESS_DISCHARGE_CMD, 0)
                    write_register(bess_client, BESS_CHARGE_CMD, 1)
                    write_register(bess_client, BESS_CHARGE_POWER_REG, charge_power)
                    print(f"Sac BESS voi cong suat: {charge_power} kW.")
                    bess_client.close()
            else:
                print(
                    "Pin da day hoac cong suat sac vuot qua kha nang cua BESS, kich hoat Zero Export."
                )
                inverter_count = len(working_inverters)
                if bess_soc > 90:
                    for inverter_client in working_inverters:
                        write_register(
                            inverter_client,
                            INVERTER_POWER_CMD,
                            (total_solar_production + grid_power) / inverter_count,
                        )
                        inverter_client.close()
                else:
                    bess_client = connect_modbus_device(BESS_IP)
                    if bess_client:
                        write_register(bess_client, BESS_DISCHARGE_CMD, 0)
                        write_register(bess_client, BESS_CHARGE_CMD, 1)
                        write_register(
                            bess_client, BESS_CHARGE_POWER_REG, bess_charge_power
                        )
                        print(f"Sac BESS voi cong suat: {bess_charge_power} kW.")
                        bess_client.close()
                    for inverter_client in working_inverters:
                        write_register(
                            inverter_client,
                            INVERTER_POWER_CMD,
                            (total_solar_production + grid_power + bess_charge_power)
                            / inverter_count,
                        )
                        inverter_client.close()

        time.sleep(5)

except KeyboardInterrupt:
    print("Dung he thong...")
finally:
    load_meter_client.close()
