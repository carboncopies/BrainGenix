###########################################################
## This file is part of the BrainGenix Simulation System ##
###########################################################

'''
Name: Socket Interface
Description: This provides a level of abstraction between SocketIO operations and the Queue Backend system.
Date-Created: 2021-04-30
'''

import multiprocessing
import atexit



class ConsumerQueue():

    def __init__(self, Logger:object, Host:str, TopicName:str, Queue:object, GroupID:str='0', Timeout:int = 6000, Offset:str='None'):

        # Create Local Vars #
        self.TopicName = TopicName
        self.TopicPartition = TopicPartition
        self.Host = Host
        self.GroupID = GroupID
        self.Offset = Offset
        self.Timeout = Timeout

        self.Logger = Logger

        # Create Queue #
        self.Queue = Queue

        # Create Consumer #
        Configuration = {'bootstrap.servers': self.Host,
                        'group.id': self.GroupID,
                        'session.timeout.ms': self.Timeout,
                        }

        if Offset != 'None':
            Configuration.update({'auto.offset.reset': Offset})


        self.ConsumerInstance = Consumer(Configuration)

        self.ConsumerInstance.subscribe([TopicName])


        # Register Exit Function #
        atexit.register(self.Shutdown, self)

        # Start Main Loop #
        self.Main()


    def Main(self): # Main Loop For Process #

        # Enter Main Loop #
        while True:

            self.ConsumerInstance.subscribe([self.TopicName])
            Message = self.ConsumerInstance.poll(timeout=1.0)

            if Message is None:
                continue

            if Message.error():
                self.Logger.Log(f'Exception In KafkaConsumerObject: {Message.error()}', 1)

            else:

                self.Queue.put(Message.value())


    def Shutdown(self): # Destroys Connection Object #

        # Shutdown Connection #
        self.ConsumerInstance.close()
        self.Logger.Log(f'Consumer Instance Exited At Address {str(hex(id(self)))}')


class ProducerQueue(): # Provides A Queue Based Interface To Send Messages To The Kafka Broker At The Specified Topic #

    def __init__(self, Logger:object, Host:str, TopicName:str, Queue:object, Partition:int): # Initialization #

        # Create Local Vars #
        self.Logger = Logger
        self.Host = Host
        self.TopicName = TopicName
        self.Queue = Queue
        self.Partition = Partition

        # Create Producer Object #
        Configuration = {'bootstrap.servers' : Host}
        self.ProducerObject = Producer(Configuration)

        # Register Exit Function #
        atexit.register(self.Shutdown, self)

        # Launch Queue Handler #
        self.QueueHandler()


    def QueueHandler(self): # Queue Handler Function #

        # Enter Main Loop #
        while True:

            # Produce Message #
            Message = self.Queue.get()
            self.ProducerObject.produce(self.TopicName, Message)


    def Shutdown(self): # Destroys Connection Object #

        # Shutdown Connection #
        self.ProducerObject.close()
        self.Logger.Log(f'Producer Instance Exited At Address {str(hex(id(self)))}')


class KafkaInterface(): # Provides An Interface To Kafka Via The Confluent-Kafka Module #

    def __init__(self, Logger:object, KafkaConfigDict:dict): # Initialize Iface #

        # Extract Values From Dictionary #
        Host = str(KafkaConfigDict.get('KafkaHost'))
        Port = str(KafkaConfigDict.get('KafkaPort'))

        Host += f':{Port}'


        # Create Local Vars #
        self.Logger = Logger
        self.Host = Host

        # Create List For Processes and Queues #
        self.ConsumerProcesses = []
        self.ProducerProcesses = []

        self.ConsumerQueues = []
        self.ProducerQueues = []

        # Create Admin Client For Later Use #
        self.Logger.Log('Creating Kafka Administration Client')

        Config = {'bootstrap.servers' : self.Host}
        self.AdminClient = AdminClient(Config)

        self.Logger.Log('Kafka Admin Client Connection Initialization Completed')

        # Register Shutdown Function #
        atexit.register(self.Shutdown, self)


    def CreateConsumer(self, TopicName:str, GroupID:str = '0', TimeoutMilliseconds:int = 6000, Offset='None'): # Creates Kafka Consumer #

        # Create Queue #
        QueueInstance = multiprocessing.Queue()

        # Create And Instantiate Process/Interface #
        ConsumerProcess = multiprocessing.Process(target=ConsumerQueue, args=(self.Logger, self.Host, TopicName, QueueInstance, GroupID, TimeoutMilliseconds, Offset, ))
        ConsumerProcess.start()

        # Append Instances #
        self.ConsumerProcesses.append(ConsumerProcess)
        self.ConsumerQueues.append(QueueInstance)

        # Return Queue #
        return QueueInstance


    def CreateProducer(self, TopicName:str, Partition:int = 0): # Creates Kafka Producer #

        # Create Queue #
        QueueInstance = multiprocessing.Queue()

        # Create And Instantiate Process/Interface #
        ProducerProcess = multiprocessing.Process(target=ProducerQueue, args=(self.Logger, self.Host, TopicName, QueueInstance, Partition, ))
        ProducerProcess.start()

        # Append Instances #
        self.ProducerProcesses.append(ProducerProcess)
        self.ProducerQueues.append(QueueInstance)

        # Return Functions #
        return QueueInstance


    def CreateTopic(self, TopicName:str, Partitions:int = 1): # Create New Topic #

        # Create Topic List #
        TopicList = []
        TopicList.append(NewTopic(TopicName, Partitions))

        # Push To Kafka #
        self.AdminClient.create_topics(TopicList)

        # Return Success #
        return True


    def Shutdown(self): # Destroys Connection Objects #

        # Shutdown Processes #
        self.Logger.Log(f'Shutting Down {len(self.ConsumerProcess) + len(self.ProducerProcesses)} Processes')

        for Process in self.ConsumerProcesses:
            self.Logger.Log(f'Terminating Process {Process}')
            Process.terminate()
