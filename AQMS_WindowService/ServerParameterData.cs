using System;
using System.Data;
using System.Data.SqlClient;
using System.Windows.Forms;
using System.Configuration;
using AQMSDataUpdateLibrary;
using System.Collections.Specialized;

namespace AQMSWindowsApp
{
    //Average Service for Server application
    public partial class ServerParameterData: Form
    {
        public ServerParameterData()
        {
            InitializeComponent();
        }

        
        private void btnSetTimeOut_Click(object sender, EventArgs e)
        {
            //string path = ConfigurationManager.AppSettings["LogfilePath"];
            //Log LogObj = new Log();
            try {
               
                var appSettingsSection = ConfigurationManager.GetSection("appSettings") as NameValueCollection;
                AvgDataCalculationServer clsObj = new AvgDataCalculationServer(appSettingsSection);
                 bool blnStatus = clsObj.CalculateParameterAvgs(ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["envConString"].ToString()].ConnectionString); //clsObj.InsertParameterAvgData(ConObj);
                if (blnStatus)
                    MessageBox.Show("Transfer done");
                else
                    MessageBox.Show("There was a problem with transferring data. Please contact admintrator.");
            }
            catch (Exception ex)
            {
                //LogObj.writeLog(ex.Message + "-" + ex.StackTrace + " : " + DateTime.Now.ToString(), path);
            }
        }

       
    }
}
