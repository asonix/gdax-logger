#!/usr/bin/env python

import sqlite3
import gdax
import datetime
import signal

"""
{
    'type': 'ticker',
    'sequence': 4916597431,
    'product_id': 'BTC-USD',
    'price': '11510.93000000',
    'open_24h': '12488.00000000',
    'volume_24h': '16290.77316441',
    'low_24h': '11510.93000000',
    'high_24h': '12645.83000000',
    'volume_30d': '683421.67508773',
    'best_bid': '11510.92',
    'best_ask': '11510.93'
}
"""

class MyWsClient(gdax.WebsocketClient):
    def __init__(self, channels = None, products = None, url = None):
        super().__init__(channels = channels, products = products, url = url)

    def on_open(self):
        print('starting websocket')
        self.db = "ws.db"

        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ticker'")

        if c.fetchone() == None:
            c.execute("""CREATE TABLE ticker (
                             sequence INTEGER,
                             product_id TEXT,
                             price REAL,
                             open_24h REAL,
                             volume_24h REAL,
                             low_24h REAL,
                             high_24h REAL,
                             volume_30d REAL,
                             best_bid REAL,
                             best_ask REAL,
                             time TEXT
                         )""")
        conn.close()

    def on_message(self, msg):
        if 'type' in msg:
            if msg['type'] == 'ticker':
                conn = sqlite3.connect(self.db)
                c = conn.cursor()

                insertable = (
                        msg['sequence'],
                        msg['product_id'],
                        msg['price'],
                        msg['open_24h'],
                        msg['volume_24h'],
                        msg['low_24h'],
                        msg['high_24h'],
                        msg['volume_30d'],
                        msg['best_bid'],
                        msg['best_ask'],
                        datetime.datetime.now()
                )

                c.execute("INSERT INTO ticker VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", insertable)
                conn.commit()
                conn.close()

    def on_close(self):
        print("Goodbye")

if __name__ == "__main__":
    ws_client = MyWsClient(channels = ['ticker'], products = ['BTC-USD'], url = "wss://ws-feed.gdax.com")
    ws_client.start()

    def handler(signum, frame):
        ws_client.close()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
