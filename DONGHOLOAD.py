from pymodbus.client.sync   import ModbusTcpClient
import datetime
import time
import sys
import io

# Đặt mã hóa của đầu ra console là utf-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Giới hạn dung lượng pin và thời gian xả
DISCHARGE_START = 18  # Giờ bắt đầu xả
DISCHARGE_END = 5     # Giờ kết thúc xả

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

# Hàm kiểm tra thời gian xả
def is_within_timer(start_hour, end_hour):
    now = datetime.datetime.now()
    return start_hour <= now.hour or now.hour < end_hour

# Hàm đọc giá trị từ Modbus
def read_register(client, register, count=1):
    try:
        result = client.read_holding_registers(register, count, unit=LOAD_METER_UNIT_ID)
        if result.isError():
            print(f"Không thể đọc thanh ghi {register}")
            return None
        return result.registers[0]
    except Exception as e:
        print(f"Lỗi khi đọc thanh ghi {register}: {e}")
        return None

# Hàm ghi giá trị vào Modbus
def write_register(client, register, value):
    try:
        value = round(value)
        result = client.write_register(register, value, unit=LOAD_METER_UNIT_ID)
        if result.isError():
            print(f"Không thể ghi vào thanh ghi {register}")
        else:
            print(f"Ghi thành công {value} vào thanh ghi {register}")
    except Exception as e:
        print(f"Loi khi ghi vào thanh ghi {register}: {e}")

# Hàm kết nối với inverter hoặc BESS qua TCP
def connect_modbus_device(ip):
    client = ModbusTcpClient(ip, port=MODBUS_TCP_PORT)
    if not client.connect():
        print(f"Không thể kết nối với {ip}.")
        return None
    return client

# Hàm đọc công suất và SOC từ BESS
def read_bess_data():
    bess_client = connect_modbus_device(BESS_IP)
    if not bess_client:
        return None, None, None

    bess_power = read_register(bess_client, BESS_POWER_REG)
    bess_soc = read_register(bess_client, BESS_SOC_REG)
    bess_charge_power = read_register(bess_client, BESS_CHARGE_POWER_REG)
    bess_client.close()

    return bess_power, bess_soc, bess_charge_power

# Khởi động quản lý năng lượng
print("Bat dau quan ly nang luong..")
load_meter_client = connect_modbus_device(LOAD_METER_IP)

if not load_meter_client:
    exit("Không thể kết nối với đồng hồ đo tải qua Modbus TCP.")

try:
    while True:
        load_consumption = read_register(load_meter_client, LOAD_CONSUMPTION_REG)
        if load_consumption is None:
            time.sleep(5)
            continue

        total_solar_production = 0
        working_inverters = []

        for ip in INVERTER_IPS:
            inverter_client = connect_modbus_device(ip)
            if inverter_client:
                solar_power = read_register(inverter_client, SOLAR_POWER_REG)
                if solar_power is not None:
                    total_solar_production += solar_power
                    working_inverters.append(inverter_client)
                else:
                    inverter_client.close()

        bess_power, bess_soc, bess_charge_power = read_bess_data()
        if bess_power is None or bess_soc is None or bess_charge_power is None:
            time.sleep(5)
            continue

        print(f"Solar: {total_solar_production} kW, Load: {load_consumption} kW, "
              f"BESS Power: {bess_power} kW, SOC: {bess_soc}%, Charge Power: {bess_charge_power} kW")

        if total_solar_production < load_consumption:
            deficit = load_consumption - total_solar_production
            inverter_count = len(working_inverters)

            if inverter_count > 0:
                power_per_inverter = min(total_solar_production / inverter_count, 100)
                for inverter_client in working_inverters:
                    write_register(inverter_client, INVERTER_POWER_CMD, power_per_inverter)
                    inverter_client.close()

            if is_within_timer(DISCHARGE_START, DISCHARGE_END) and bess_soc > 10:
                discharge_power = min(deficit, bess_power)
                bess_client = connect_modbus_device(BESS_IP)
                if bess_client:
                    write_register(bess_client, BESS_DISCHARGE_CMD, 1)
                    write_register(bess_client, BESS_POWER_REG, discharge_power)
                    bess_client.close()
            else:
                print("Bổ sung tải bằng lưới điện.")
        else:
            excess_energy = total_solar_production - load_consumption
            if bess_soc < 90 and excess_energy <= bess_charge_power:
                charge_power = excess_energy
                bess_client = connect_modbus_device(BESS_IP)
                if bess_client:
                    write_register(bess_client, BESS_DISCHARGE_CMD, 0)
                    write_register(bess_client, BESS_CHARGE_CMD, 1)
                    write_register(bess_client, BESS_CHARGE_POWER_REG, charge_power)
                    print(f"Sạc BESS với công suất: {charge_power} kW.")
                    bess_client.close()
            else:
                print("pin đã đầy hoặc công suất sạc vượt quá khả năng của BESS, kích hoạt Zero Export.")
                inverter_count=len(INVERTER_IPS)
                if bess_soc < 90 :
                    power_per_inverter=(load_consumption + bess_charge_power)/inverter_count
                else:
                    power_per_inverter=load_consumption/inverter_count
                for ip in INVERTER_IPS:
                    inverter_client=connect_modbus_device(ip)
                    if inverter_client :
                        write_register= inverter_client, INVERTER_POWER_CMD, power_per_inverter
                        inverter_client.close()
                
        time.sleep(5)

except KeyboardInterrupt:
    print("Dừng hệ thống...")
finally:
    load_meter_client.close()
