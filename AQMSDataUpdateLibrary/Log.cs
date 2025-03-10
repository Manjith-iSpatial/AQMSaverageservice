using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;


namespace AQMSDataUpdateLibrary
{
    public class Log
    {
        public void writeLog(string strValue, string path)
        {
            StreamWriter sw = null;
            string fileName = String.Empty;
            try
            {
                if (Path.HasExtension(path))
                {
                    fileName = path;
                }
                else
                {
                    // Determine whether the directory exists.
                    if (!Directory.Exists(path))
                    {
                        // Try to create the directory.
                        DirectoryInfo di = Directory.CreateDirectory(path);
                    }
                    fileName = path + "/AQMSServerAverageSerivceLog_" + DateTime.Now.Day.ToString() + DateTime.Now.ToString("MMM") + DateTime.Now.Year.ToString() + ".log";
                }
                if (!File.Exists(fileName))
                {
                    sw = File.CreateText(fileName);
                }
                else
                {
                    sw = File.AppendText(fileName);
                }
                LogWrite(strValue, sw);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                if (sw != null)
                {
                    sw.Flush();
                    sw.Close();
                }
            }
        }



        private static void LogWrite(string logMessage, StreamWriter w)
        {
            w.WriteLine($"{logMessage} - {DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss tt")}");
        }
    }
}
