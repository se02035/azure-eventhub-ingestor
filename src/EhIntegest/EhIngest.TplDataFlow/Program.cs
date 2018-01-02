using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace EhIngest.TplDataFlow
{
    class Program
    {
        static EventHubClient _eventhubClient;

        static BufferBlock<DataPoint> _rawMessagesBuffer;
        static TransformBlock<DataPoint, EventData> _rawMessagesConversation;
        static BufferBlock<EventData> _ehMessagesBuffer;
        static BatchBlock<EventData> _ehMessagesBatch;
        static ActionBlock<EventData[]> _ehSendAction;

        static void Main(string[] args)
        {
            var appSettings = ReadConfiguration();
            InitializeAndRunConsumer(appSettings.Item1, appSettings.Item2, appSettings.Item3);
            InitializeAndRunProducer();
        }

        private static Tuple<string, int, int> ReadConfiguration()
        {
            string ehConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];
            int bufferSize = int.Parse(ConfigurationManager.AppSettings["DataFlowBufferSize"]);
            int batchSize = int.Parse(ConfigurationManager.AppSettings["DataFlowBatchSize"]);

            return new Tuple<string, int, int>(ehConnectionString, bufferSize, batchSize);
        }

        private static void InitializeAndRunProducer()
        {
            // initialize simulator values
            Console.Write("Please enter sensor id (e.g. temperature)> ");
            var sensorId = Console.ReadLine();

            Console.Write("Please enter default value (e.g. 15) > ");
            var sensorValueDefault = int.Parse(Console.ReadLine());

            Console.Write(@"Please enter value bandwith (+/- e.g. 3) > ");
            var sensorValueBandwith = int.Parse(Console.ReadLine());

            Console.Write("Press key to start simulator");
            Console.ReadKey();

            // start the simulator
            Thread producer = new Thread(Produce);
            producer.Start(new Tuple<string, int, int>(sensorId, sensorValueDefault, sensorValueBandwith));
        }

        private static void InitializeAndRunConsumer(string ehConnectionString, int bufferSize, int batchSize)
        {
            InitializeEvenhubClient(ehConnectionString);
            InitializeDataFlow(bufferSize, batchSize);
        }

        private static void InitializeEvenhubClient(string ehConnectionString)
        {
            _eventhubClient = EventHubClient.CreateFromConnectionString(ehConnectionString);
        }

        private static void InitializeDataFlow(int bufferSize, int batchSize)
        {
            _rawMessagesBuffer = new BufferBlock<DataPoint>(new DataflowBlockOptions()
            {
                BoundedCapacity = bufferSize
            });

            _rawMessagesConversation = new TransformBlock<DataPoint, EventData>(
                (dp) => new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(dp))),
                new ExecutionDataflowBlockOptions()
                {
                    //SingleProducerConstrained = true,
                    BoundedCapacity = bufferSize
                });

            _ehMessagesBuffer = new BufferBlock<EventData>(new DataflowBlockOptions()
            {
                BoundedCapacity = bufferSize
            });

            _ehMessagesBatch = new BatchBlock<EventData>(batchSize, new GroupingDataflowBlockOptions()
            {
                BoundedCapacity = batchSize,
                Greedy = true
            });

            _ehSendAction = new ActionBlock<EventData[]>(
                async (payload) =>
                {
                    await SendData(payload);
                    Console.WriteLine($"Consumer - {DateTime.Now.ToLongTimeString()} > New batch sent");
                },
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = bufferSize,
                    SingleProducerConstrained = true
                });

            _rawMessagesBuffer.LinkTo(_rawMessagesConversation);
            _rawMessagesConversation.LinkTo(_ehMessagesBuffer);
            _ehMessagesBuffer.LinkTo(_ehMessagesBatch);
            _ehMessagesBatch.LinkTo(_ehSendAction);
        }

        private static async Task SendData(EventData[] data)
        {
            try
            {
                await _eventhubClient.SendBatchAsync(data);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static void Produce(object parameters)
        {
            var producerParams = parameters as Tuple<string, int, int>;
            if (producerParams == null)
                throw new ArgumentNullException(nameof(parameters));

            var sensorId = producerParams.Item1;
            var sensorValueDefault = producerParams.Item2;
            var sensorValueBandwidth = producerParams.Item3;

            var rand = new Random();
            var minVal = sensorValueDefault - sensorValueBandwidth;
            var maxVal = sensorValueDefault + sensorValueBandwidth;

            // create new simulated datapoints
            while (true)
            {
                var payload = new DataPoint() { Id = sensorId, Value = rand.Next(minVal, maxVal) };
                payload.Ts = DateTime.Now;

                Console.WriteLine($"Producer - {DateTime.Now.ToLongTimeString()} > Adding new item. Current queue count: {_rawMessagesBuffer.Count}");

                _rawMessagesBuffer.Post(payload);
            }
        }
    }
}
