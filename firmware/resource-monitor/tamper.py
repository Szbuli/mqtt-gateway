import threading
import time
import RPi.GPIO as GPIO


class Tamper:
    def __init__(self, mqtt, tamperTopic):
        self.lock = threading.Lock()
        self.mqtt = mqtt
        self.tamperTopic = tamperTopic
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(0, GPIO.IN, pull_up_down=GPIO.PUD_OFF)
        GPIO.add_event_detect(
            0, GPIO.BOTH, callback=self.tamper_event)
        self.tamper_event_happened = None

    def tamper_event(self, channel):
        self.lock.release()

    def start(self):
        t = threading.Thread(target=self.check_tamper)
        t.setDaemon(True)
        t.start()

    def check_tamper(self):
        while True:
            self.lock.acquire()
            checks = 0
            while checks < 5:
                checks += 1
                tamper_state = GPIO.input(0)
                time.sleep(0.3)
                if self.tamper_event_happened != tamper_state:
                    self.tamper_event_happened = tamper_state
                    self.mqtt.publish(self.tamperTopic,
                                      "ON" if tamper_state == 1 else "OFF")
