from confluent_kafka import Consumer
import socket
import os
import argparse

class Consume :
    
    def __init__(self, bootstrap, topic, print_nth_msg):
        self.name = "Consume"
        self.topic = topic
        self.nthMsg = print_nth_msg
        self.bootstrap = bootstrap
        self.msgCount = 0

    def intializeKafka(self) :
        bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")

        if bootstrap_servers is None:
            print("BOOTSTRAP_SERVERS environment variable not set. Using commandline argument for bootstrap server.")
            bootstrap_servers = self.bootstrap
        
        print("Bootstrap servers: " + bootstrap_servers)

        
        conf = {'bootstrap.servers': bootstrap_servers,
        'group.id': 'consumer-1',
        'client.id': socket.gethostname(),
        'auto.offset.reset': 'earliest'}

        self.consumer = Consumer(conf)

        # Subscribe to topic
        self.consumer.subscribe([self.topic])


    def run(self):
        print ("Listening for events on topic: " + self.topic  + ". Press Ctrl-C to exit.")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                self.msgCount += 1
                if (self.msgCount < 2 or self.msgCount % int(self.nthMsg) == 0) :
                    print('Received message: {}'.format(msg.value().decode('utf-8')))
                
        except KeyboardInterrupt:
            pass
        

def main():
    topic = None
    nthMsg = None
    parser = argparse.ArgumentParser(description='Consume events from Kafka.')
    parser.add_argument('-t', '--topic', help='Topic to consume from.', required=True, metavar="")
    parser.add_argument('-b', '--bootstrap', help='Bootstrap server. Defaults to localhost:9092.', required=False, metavar="")
    parser.add_argument('-p', '--print_n_msg', help='Print every nth msg consumed. Defaults to every msg.', required=False, metavar="")

    
    args = parser.parse_args()
    bootstrap = "localhost:9092" if args.bootstrap is None else args.bootstrap
    topic = args.topic
    nthMsg = 1 if args.print_n_msg is None else args.print_n_msg
    
    c = Consume(bootstrap, topic, nthMsg)
    
    c.intializeKafka()

    input("We are ready, press the enter key to continue")

    c.run()

    c.consumer.close()
    
    input("Press the enter key to exit the consume application.")

    

if __name__ == "__main__":
    main()