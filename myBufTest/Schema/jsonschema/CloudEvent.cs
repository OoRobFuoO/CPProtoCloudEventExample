using System;
using Newtonsoft.Json;

namespace RF.MyBufTest.Schema.JsonSchema
{
    public class CloudEvent
    {
        [JsonRequired]
        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("source")]
        public string source { get; set; }

        [JsonRequired]
        [JsonProperty("specVersion")]
        public string specVersion { get; set; }

        [JsonRequired]
        [JsonProperty("type")]
        public string type { get; set; }

        [JsonProperty("subject")]
        public string subject { get; set; }

        [JsonRequired]
        [JsonProperty("time")]
        public DateTime time { get; set; }

        [JsonRequired]
        [JsonProperty("datacontenttype")]
        public string datacontenttype { get; set; }

        [JsonRequired]
        public object data { get; set; }        
    }

    public class CloudEventData
    {
        private const string DefaultPrefix = "type.googleapis.com";

        [JsonProperty("@type")]
        public string type { get { return $"{DefaultPrefix}/{eventType}"; } }

        public virtual string eventType { get; set; }
    }
}
