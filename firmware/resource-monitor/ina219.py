import board
import logging
import busio
from adafruit_ina219 import ADCResolution, BusVoltageRange, INA219 as LIB_INA219


class INA219:
    def __init__(self, mqtt, voltageTopic, currentTopic, powerTopic):
        self.voltageTopic = voltageTopic
        self.currentTopic = currentTopic
        self.powerTopic = powerTopic
        self.mqtt = mqtt
        i2c = busio.I2C(board.SCL, board.SDA)
        self.ina219 = LIB_INA219(i2c)

        if self.ina219.overflow:
            print("Internal Math Overflow Detected!")

        self.ina219.bus_adc_resolution = ADCResolution.ADCRES_12BIT_128S
        self.ina219.shunt_adc_resolution = ADCResolution.ADCRES_12BIT_128S
        self.ina219.bus_voltage_range = BusVoltageRange.RANGE_16V


    def read_and_publish(self):
        bus_voltage = self.ina219.bus_voltage
        current = self.ina219.current
        power = self.ina219.power
        self.mqtt.publish(self.voltageTopic, str(round(bus_voltage, 2)))
        self.mqtt.publish(self.currentTopic, str(round(current, 2)))
        self.mqtt.publish(self.powerTopic, str(round(power, 2)))
