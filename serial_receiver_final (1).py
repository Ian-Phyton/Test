# Will run by Raspberry Pi 4
# Receive sensor radio data at the serial port
# Decode sensor data and write into data_manager.py
from enocean.consolelogger import init_logging
import enocean.utils
from enocean.communicators.serialcommunicator import SerialCommunicator
from enocean.protocol.packet import RadioPacket
from enocean.protocol.constants import PACKET, RORG
import os
from enocean.protocol.eep import EEP
from datetime import datetime
import csv
import psycopg2

connection = psycopg2.connect(
    user="dbfirenet_user", 
    password="G9Fw38n8WjMfN4zTBydkxYqZFefZSiM4", 
    host="dpg-ck0lntu3ktkc73f98tcg-a.singapore-postgres.render.com", 
    port="5432", 
    database="dbfirenet"
)
cursor = connection.cursor()

address = {
    "42261f3" : [1, 1407, "Blk A01 #01-1407", "S760A01"],
    "422607c" : [1, 1408, "Blk B01 #01-1408", "S760B01"],
    "42262a" : [2, 1407, "Blk C01 #02-2407", "S760C01"],
    "4226087" : [2, 1408, "Blk D01 #02-2408", "S760D01"],
    "4226222" : [3, 1407, "Blk E01 #03-3407", "S760E01"],
    "42261ea" : [3, 1408, "Blk F01 #03-3408", "S760F01"],
}

try:
    import queue
except ImportError:
    import Queue as queue


with open('data_manager.csv', 'w') as csv_file:
    csvwriter = csv.writer(csv_file)
    Column_list = ["Date", "Time", "Sensor ID", "Temperature", "Acceleration X", "Acceleration Y", "Acceleration Z", "Magnet Contact"]
    csvwriter.writerow(Column_list)


init_logging()
communicator = SerialCommunicator(port='COM3')
communicator.start()
print('The Base ID of your module is %s.' % enocean.utils.to_hex_string(communicator.base_id))

# endless loop receiving radio packets
while communicator.is_alive():
    try:
        # Loop to empty the queue...
        packet = communicator.receive.get(block=True, timeout=0.5)

        if packet.packet_type == PACKET.RADIO_ERP1 and packet.rorg == RORG.VLD:
            
            # Obtain the sensor_id, its always located at the back of packet data but varies with packet type, for VLD, its the last 6th to last 2nd, thus [-5:-1]
            packet_id = packet.data[-5:-1]         
            packet_id.append('')
            for i in range(len(packet_id) - 1):
                packet_id[len(packet_id) - 1] += str(hex(packet_id[i]).split('x')[-1])
            
            sensor_id = packet_id[len(packet_id) - 1]
            print(sensor_id)                                #Display for testing(Success)

            current_datetime = datetime.now()
            current_time = current_datetime.strftime("%H:%M:%S")
            current_day = current_datetime.strftime("%Y/%m/%d")

            # Get temperature from eep packet
            packet.select_eep(0x14, 0x41)
            packet.parse_eep()
            for k in packet.parsed:

                if k == 'TMP':
                    TMPdata = packet.parsed[k]
                    TMP = TMPdata['value']
                
                elif k == 'ACX':
                    ACXdata = packet.parsed[k]
                    ACX = ACXdata['value']
                
                elif k == 'ACY':
                    ACYdata = packet.parsed[k]
                    ACY = ACYdata['value']
                
                elif k == 'ACZ':
                    ACZdata = packet.parsed[k]
                    ACZ = ACZdata['value']

                elif k == 'CO':
                    COdata = packet.parsed[k]
                    CO = COdata['value']

            # with open('data_manager.csv', 'w') as csv_file:
            #     # csvwriter = csv.writer(csv_file)
            #     # csvwriter.writerow([current_day, current_time, sensor_id, TMP, ACX, ACY, ACZ, CO])
            create_script = ''' CREATE TABLE IF NOT EXISTS data (
            date TIMESTAMP,
            sensorID VARCHAR(10),
            temperature REAL,
            x REAL,
            y REAL,
            z REAL,
            magnet_contact VARCHAR(7),
            level INT,
            block TEXT,
            postal_code TEXT,
            unit INT
            )'''
            
            cursor.execute(create_script)
            connection.commit()
            
            time = current_day + " " + current_time

            level = address[sensor_id][0]
            unit = address[sensor_id][1]
            block = address[sensor_id][2]
            postal_code = address[sensor_id][3]

            new_data = tuple([time, sensor_id, TMP, ACX, ACY, ACZ, CO, level, unit, block, postal_code])
            cursor.execute(f"""INSERT INTO data (date, sensorID, temperature, x, y, z, magnet_contact, level, unit, block, postal_code) VALUES
                {new_data}
            """)
            connection.commit()   
                

    except queue.Empty:
        continue
    except KeyboardInterrupt:
        break
    except Exception:
        traceback.print_exc(file=sys.stdout)
        break

if communicator.is_alive():
    communicator.stop()
