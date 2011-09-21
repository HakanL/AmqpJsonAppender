using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;


namespace Haukcode.AmqpJsonAppender 
{
    public class AmqpTransport
    {
        private static object locker = new object();
        private bool stopped;
        private ConnectionFactory factory;
        private IConnection connection;
        private IModel model;

        public string VirtualHost { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public string Queue { get; set; }
        public string IpAddress { get; set; }
        public int Port { get; set; }


        public AmqpTransport()
        {
        }


        public void Close()
        {
            stopped = true;
            lock (locker)
            {
                if (model != null)
                    model.Close(200, "Shutdown");

                if (connection != null)
                    connection.Close(200, "Shutdown");

                factory = null;
            }
        }


        private ConnectionFactory Factory
        {
            get
            {
                if (factory != null)
                    return factory;

                lock (locker)
                {
                    factory = new ConnectionFactory()
                    {
                        Protocol = Protocols.FromEnvironment(),
                        HostName = IpAddress,
                        Port = Port,
                        VirtualHost = VirtualHost,
                        UserName = User,
                        Password = Password
                    };

                    return factory;
                }
            }
        }


        private IConnection Connection
        {
            get
            {
                if (connection != null)
                    return connection;

                lock (locker)
                {
                    connection = Factory.CreateConnection();

                    return connection;
                }
            }
        }


        private IModel Model
        {
            get
            {
                if (model != null)
                    return model;

                lock (locker)
                {
                    model = Connection.CreateModel();

                    return model;
                }
            }
        }


        private void Reset()
        {
            lock (locker)
            {
                // Reset
                if (model != null)
                {
                    try
                    {
                        model.Close();
                    }
                    catch
                    {
                    }
                }
                model = null;

                if (connection != null)
                {
                    try
                    {
                        connection.Close();
                    }
                    catch
                    {
                    }
                }
                connection = null;

                factory = null;
            }
        }

        public void Send(string message)
        {
            try
            {
                var model = Model;
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 1;

                if (!stopped)
                    model.BasicPublish(Queue, "elasticsearch", props, Encoding.UTF8.GetBytes(message));
            }
            catch(RabbitMQ.Client.Exceptions.BrokerUnreachableException)
            {
                Reset();
                // Back off
                System.Threading.Thread.Sleep(5000);
            }
            catch
            {
                Reset();
            }
        }
    }
}
