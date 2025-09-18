namespace DataBridge.Models.Excels
{
    public class ExportRequest
    {
        public string ConnectionString { get; set; }
        public string SqlQuery { get; set; }
        public string FileName { get; set; }
    }
}
