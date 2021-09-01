using System;
using Newtonsoft.Json;
using RF.MyBufTest.Schema.Data;

namespace RF.MyBufTest.Schema.JsonSchema
{
    public class User : CloudEventData
    {
        [JsonIgnore]
        public override string eventType { get { return "RF.myBufTest.schema.data.User"; } }

        public string userID { get; set; }

        public string firstname { get; set; }

        public string lastname { get; set; }
    }
}
