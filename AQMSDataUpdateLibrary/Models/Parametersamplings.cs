using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AQMSDataUpdateLibrary.Models
{
    public class Parametersamplings
    {
        public Parametersamplings()
        {

        }

        public int ID { get; set; }
        public int StationID { get; set; }
        public int DeviceID { get; set; }
        public int ParameterID { get; set; }
        public double? Parametervalue { get; set; }
        public int? LoggerFlags { get; set; }
        public string Alarm { get; set; }
        public bool? IsLocked { get; set; }
        public string AqsMethod { get; set; }
        public DateTime CreatedTime { get; set; }
        public int? CreatedBy { get; set; }
        public DateTime? ModifyOn { get; set; }
        public int? ModifyBy { get; set; }
        public string StationGUID { get; set; }
        public DateTime Interval { get; set; }
        public int ParameterIDRef { get; set; }

        public double? ParametervalueOrginal { get; set; }
        public int? LoggerFlagsOriginal { get; set; }
    }
}
