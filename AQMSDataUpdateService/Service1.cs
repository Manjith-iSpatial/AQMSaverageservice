using AQMSDataUpdateLibrary;
using System;
using System.Collections.Specialized;
using System.Configuration;
using System.ServiceProcess;
using System.Timers;

namespace AQMSDataUpdateService
{
    //Average Service for Server application
    public partial class Service1 : ServiceBase
    {
        Timer timer1 = new Timer();
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            //Log LogObj = new Log();
            //LogObj.writeLog("Average Data service Started: " + DateTime.Now.ToString(),ConfigurationManager.AppSettings["LogfilePath"].ToString());
            timer1.Elapsed += new ElapsedEventHandler(timer1_Elapsed);
            timer1.Interval = Convert.ToDouble(ConfigurationManager.AppSettings["Interval"].ToString());
            timer1.Enabled = true;
            timer1.Start();
        }

        protected override void OnStop()
        {
            timer1.Stop();
            //Log LogObj = new Log();
            //LogObj.writeLog("Average Data service stopped: " + DateTime.Now.ToString(),ConfigurationManager.AppSettings["LogfilePath"].ToString());
        }

        private void timer1_Elapsed(object sender, EventArgs e)
        {
           // string path = ConfigurationManager.AppSettings["LogfilePath"];
            //Log LogObj = new Log();
            try
            {
                string strConnectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["envConString"].ToString()].ConnectionString;
                var appSettingsSection = ConfigurationManager.GetSection("appSettings") as NameValueCollection;
                timer1.Stop();
                AvgDataCalculationServer clsObj = new AvgDataCalculationServer(appSettingsSection);
                bool blnStatus = clsObj.CalculateParameterAvgs(strConnectionString); //clsObj.InsertParameterAvgData(ConObj);
                //if (!blnStatus)
                //    LogObj.writeLog("There was some problem with transfer data. Please contact administrator",path);
            }
            catch (Exception ex)
            {
                timer1.Start();
               // LogObj.writeLog("Error: " + ex.StackTrace + "\n\n" + ex.Message + DateTime.Now.ToString(),path);
            }
            finally
            {
                timer1.Start();
            }
        }
    }
}
