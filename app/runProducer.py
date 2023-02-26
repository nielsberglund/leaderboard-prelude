from confluent_kafka import Producer
import argparse
import socket
# import sys
import os
import gameplay
import random
import time

class Produce:
    
    def __init__(self, doLoop, numberLoops, minLatency, maxLatency, bootstrap, topic, print_nth_msg):
        self.name = "Produce"
        self.doLoop = doLoop
        self.numberLoops = numberLoops
        self.minLatency = minLatency
        self.maxLatency = maxLatency
        self.topic = topic
        self.nthMsg = print_nth_msg
        self.bootstrap = bootstrap
        self.numMsg = 0

    def intializeKafka(self) :
        bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")

        if bootstrap_servers is None:
            print("BOOTSTRAP_SERVERS environment variable not set. Using commandline argument for bootstrap server.")
            bootstrap_servers = self.bootstrap
        
        print("Bootstrap servers: " + bootstrap_servers)
        
        conf = {'bootstrap.servers': bootstrap_servers,
                'client.id': socket.gethostname()}

        
        self.producer = Producer(conf)
    
    
    def acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
          if self.doLoop == False :
            print('Message produced: {}'.format(msg.value().decode('utf-8')))
          else:      
            if (self.numMsg < 2 or self.numMsg % int(self.nthMsg) == 0) :
              print('Message produced: {}'.format(msg.value().decode('utf-8')))
              

    def generateAndPublish(self, topic) :
        keyVal, jsonObj = gameplay.generateGamePlay()
        self.producer.produce(topic, key=str(keyVal), value=jsonObj, callback=self.acked)
        self.producer.poll(1)

    def run(self):
        if not self.doLoop :
          while(True) :
            val = input("Press 'Y' to continue, 'N' to exit.")
            
            if val == "Y":
              print ("Generating and publishing event")
              self.generateAndPublish(self.topic)
                  
            if val == "N":
              break
        
        if(self.doLoop):
          print ("Generating and publishing event in loop")
          for i in range(int(self.numberLoops)) :
            self.numMsg += 1
            latency = random.randint(int(self.minLatency), int(self.maxLatency))
            self.generateAndPublish(self.topic)
            time.sleep(latency / 1000)
        
        if(self.doLoop):
            print("Finished publishing: " + str(self.numMsg) + " messages.")

    
def main():
    numberLoops = None
    minLatency = None
    maxLatency = None
    topic = None
    nthMsg = None
    bootstrap = None

    parser = argparse.ArgumentParser(description='Generate and publish events to Kafka.')
    parser.add_argument('-t', '--topic', help='Topic to publish to', required=True, metavar="")
    parser.add_argument('-b', '--bootstrap', help='Bootstrap server. Defaults to localhost:9092.', required=False, metavar="")
    parser.add_argument('-l', '--loop', default=False, action="store_true", help='If set, then run in a loop.', required=False)
    parser.add_argument('-e', '--events', help='When running in a loop, number of events to generate.', required=False, metavar="")
    parser.add_argument('-m', '--min_latency', help='When running in a loop, minimum latency. Defaults to 500 ms.', required=False, metavar="")
    parser.add_argument('-x', '--max_latency', help='When running in a loop, maximum latency. Defaults to 1000 ms.', required=False, metavar="")
    parser.add_argument('-p', '--print_n_msg', help='When running in a loop, print every nth msg. Defaults to very 100th.', required=False, metavar="")

    args = parser.parse_args()
    topic = args.topic
    doLoop = args.loop
    bootstrap = "localhost:9092" if args.bootstrap is None else args.bootstrap
    if doLoop:
        numberLoops = 10 if args.events is None else args.events
        minLatency = 500 if args.min_latency is None else args.min_latency    
        maxLatency = 1000 if args.max_latency is None else args.max_latency
        nthMsg = 100 if args.print_n_msg is None else args.print_n_msg
        print("Running in a loop. Number loops: " + str(numberLoops) + " Min latency: " + str(minLatency) + " Max latency: " + str(maxLatency) + " Printing every " + str(nthMsg) + ":th message.")
        
    input("We are ready, press the enter key to continue")
    p = Produce(doLoop, numberLoops, minLatency, maxLatency, bootstrap, topic, nthMsg)
    
    p.intializeKafka()

    p.run()
    
    input("Press the enter key to exit")

if __name__ == "__main__":
    main()