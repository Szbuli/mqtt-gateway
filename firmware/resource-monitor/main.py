import time
import mqtt_manager
import logging
import configparser
from rpi_status import RpiStatus
from bme280 import BME280
from tamper import Tamper
from ina219 import INA219

from timeloop import Timeloop
from datetime import timedelta

tl = Timeloop()
rpi_status: RpiStatus = None
bme_280: BME280 = None
tamper: Tamper
ina219: INA219 = None


def start():
    config = configparser.ConfigParser()
    config.read('settings.ini')

    logging.basicConfig(level=logging.getLevelName(
        config['logger']['logLevel']))

    mqtt_client = mqtt_manager.init(config['mqtt']['host'],
                                    config['mqtt']['port'],
                                    config['mqtt']['username'],
                                    config['mqtt']['password'])

    if (config.has_option('rpi', 'coreTempTopic') and
        config.has_option('rpi', 'averageLoadTopic') and
        config.has_option('rpi', 'diskUsageTopic')
        ):
        global rpi_status
        rpi_status = RpiStatus(
            mqtt_client,
            config['rpi']['coreTempTopic'],
            config['rpi']['averageLoadTopic'],
            config['rpi']['diskUsageTopic'])

    if config.has_option('sensors', 'temperatureTopic') and config.has_option('sensors', 'humidityTopic'):
        global bme_280
        bme_280 = BME280(mqtt_client, config['sensors']['temperatureTopic'],
                         config['sensors']['humidityTopic'])

    if config.has_option('sensors','tamperTopic'):
        global tamper
        tamper = Tamper(mqtt_client, config['sensors']['tamperTopic'])
        tamper.start()

    if config.has_option('sensors','voltageTopic') and config.has_option('sensors', 'currentTopic') and config.has_option('sensors', 'powerTopic'):
        global ina219
        ina219 = INA219(mqtt_client, config['sensors']['voltageTopic'], config['sensors']['currentTopic'], config['sensors']['powerTopic'])

    tl.start()

    mqtt_manager.loop()


@tl.job(interval=timedelta(seconds=60))
def read_and_publish():
    if rpi_status != None:
        rpi_status.read_and_publish()

    if bme_280 != None:
        bme_280.read_and_publish()

    if ina219 != None:
        ina219.read_and_publish()


if __name__ == '__main__':
    start()
