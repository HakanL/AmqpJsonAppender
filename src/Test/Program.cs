using System;
using System.Collections.Generic;
using System.Text;


namespace Haukcode.AmqpJsonAppender.Test
{
    public class Program
    {
        // Create a logger for use in this class
        private static readonly log4net.ILog log =
            log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


        public static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();

            for(int index = 0; index < 10; index++)
                log.InfoFormat("Hello world \"{0}\"  \\\nHej", index + 1);

            log.Info("Result string: ResponseStatus=SUCCESS&ResponseMessage=Success&ResponseTIBCount=1&TransactionID=&TransactionStatus=COMPLETED&TransactionResult=0&TransactionMessage=Approved&TransactionStartDate=Mon+Sep+19+17%3A35%3A13+PDT+2011&TransactionLastDate=Mon+Sep+19+17%3A35%3A13+PDT+2011&TransactionDuration=13&AccountID=1sfsdfasd3sfd1fs6sf4d7WJVDJIDL&ClientTransactionID=");
            System.Threading.Thread.Sleep(2000);

            Console.WriteLine("Closing down");

            log4net.LogManager.Shutdown();
        }
    }
}
