# coding=utf-8

"""
Send metrics to Circonus

#### Dependencies

 * urllib2


#### Configuration
Enable this handler

 * handers = diamond.handler.circonus.CirconusHandler

"""

from Handler import Handler
from diamond.util import get_diamond_version
import json
import time
import urllib2


class CirconusHandler(Handler):

    # Inititalize Handler with url and batch interval
    def __init__(self, config=None):
        Handler.__init__(self, config)
        self.metrics = []
        self.url = self.config['url']
        self.batch_interval = self.config['batch_interval']
        self.resetBatchTimeout()

    def resetBatchTimeout(self):
        self.batch_max_timestamp = int(time.time() + self.batch_interval)

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(CirconusHandler, self).get_default_config_help()

        config.update({
            'url': 'Where to send metrics',
            'batch_interval': 'Interval to send metrics'
            })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(CirconusHandler, self).get_default_config()

        config.update({
            'url': '',
            'batch_interval': 60,
            })

        return config

    def process(self, metric):
        """
        Queue a metric.  Flushing queue if batch size reached
        """
        self.metrics.append(metric)
        if self.should_flush():
            self._send()

    def should_flush(self):
        return time.time() >= self.batch_max_timestamp

    def flush(self):
        """Flush metrics in queue"""
        if self.should_flush():
            self._send()

    def user_agent(self):
        """
        HTTP user agent
        """
        return "Diamond: %s" % get_diamond_version()

    def _send(self):
        metric_data = {}
        for metric in self.metrics:
            metric_data['%s.%s' % (
                metric.getCollectorPath(),
                metric.getMetricPath()
            )] = metric.value

        self.metrics = []
        metric_json = json.dumps(metric_data)
        self.log.debug("Body is %s", metric_json)
        req = urllib2.Request(self.url, metric_json,
                              {"Content-type": "application/json",
                               "User-Agent": self.user_agent()})
        self.resetBatchTimeout()
        try:
            urllib2.urlopen(req)
        except urllib2.URLError:
            self.log.error("Unable to post metrics to provided URL")
            return
