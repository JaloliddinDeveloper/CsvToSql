using ExcelDataReader;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DataBridge.Controllers
{
    public class ExcelController:Controller
    {
        private readonly IConfiguration configuration;

        public ExcelController(IConfiguration configuration) =>
            this.configuration = configuration;

        public IActionResult UploadExcel() => View();

        [HttpPost]
        public async Task<IActionResult> UploadExcel(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                ViewBag.Message = "❌ Iltimos, Excel fayl yuklang.";
                return View();
            }

            string tableName = Path.GetFileNameWithoutExtension(file.FileName);
            string connStr = this.configuration.GetConnectionString("DefaultConnection");

            try
            {
                System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();

                using var stream = file.OpenReadStream();
                using var reader = ExcelReaderFactory.CreateReader(stream);

                DataTable dt = new();
                bool headersAdded = false;
                int batchSize = 100000;
                int currentCount = 0;

                string dropIfExists = $@"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE [{tableName}]";
                using (var cmd = new SqlCommand(dropIfExists, conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                while (reader.Read())
                {
                    if (!headersAdded)
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string colName = reader.GetValue(i)?.ToString()?.Trim();
                            if (string.IsNullOrEmpty(colName))
                                colName = "Column" + i;

                            dt.Columns.Add(colName, typeof(string));
                        }

                        StringBuilder createTable = new($"CREATE TABLE [{tableName}] (");
                        foreach (DataColumn col in dt.Columns)
                        {
                            createTable.Append($"[{col.ColumnName}] NVARCHAR(MAX),");
                        }
                        createTable.Length--;
                        createTable.Append(");");

                        using (var cmd = new SqlCommand(createTable.ToString(), conn))
                        {
                            await cmd.ExecuteNonQueryAsync();
                        }

                        headersAdded = true;
                    }
                    else
                    {
                        object[] values = new object[dt.Columns.Count];
                        for (int i = 0; i < dt.Columns.Count; i++)
                        {
                            values[i] = reader.GetValue(i)?.ToString() ?? "";
                        }
                        dt.Rows.Add(values);
                        currentCount++;

                        if (currentCount >= batchSize)
                        {
                            await BulkInsertAsync(conn, dt, tableName);
                            dt.Clear();
                            currentCount = 0;
                        }
                    }
                }

                if (dt.Rows.Count > 0)
                {
                    await BulkInsertAsync(conn, dt, tableName);
                }

                ViewBag.Message = $"✅ Fayl '{file.FileName}' muvaffaqiyatli yuklandi va '{tableName}' jadvaliga yozildi!";
            }
            catch (Exception ex)
            {
                ViewBag.Message = "❌ Xatolik: " + ex.Message;
            }

            return View();
        }

        private async Task BulkInsertAsync(SqlConnection conn, DataTable dt, string tableName)
        {
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null)
            {
                DestinationTableName = tableName,
                BulkCopyTimeout = 0,
                BatchSize = dt.Rows.Count
            };

            foreach (DataColumn col in dt.Columns)
            {
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }

            await bulk.WriteToServerAsync(dt);
        }
    }
}
