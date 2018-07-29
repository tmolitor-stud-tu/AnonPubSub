from threading import Timer, Event, current_thread
import logging
logger = logging.getLogger(__name__)


class LED(object):
    def __init__(self, led_id, node_id, event_queue):
        self.led_id = led_id
        self.node_id = node_id
        self.event_queue = event_queue
        self.led_timer = None
    
    def on(self, color, off_time=None):
        if off_time:
            if self.led_timer:
                self.led_timer.cancel()
            self.led_timer = Timer(off_time, self.off)
            self.led_timer.start()
        else:
            self.led_timer = None
        # *** ADD HERE: code to turn on physical led with color specified in color tuple
        self.event_queue.put({"type": "led", "data": {"led_id": self.led_id, "color": "rgb%s" % str(color)}})
    
    def off(self):
        # *** ADD HERE: code to turn off physical led
        self.event_queue.put({"type": "led", "data": {"led_id": self.led_id, "color": ""}})

    
def init(node_id, event_queue):
    logger.info("Initializing LEDs...")
    leds = []
    for led_id in range(0, 10):
        leds.append(LED(led_id, node_id, event_queue))
    return leds
