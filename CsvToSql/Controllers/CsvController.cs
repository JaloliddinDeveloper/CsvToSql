using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CsvToSql.Controllers
{
    public class CsvController:Controller
    {
        private readonly IConfiguration configuration;

        public CsvController(IConfiguration configuration) =>
            this.configuration = configuration;

        public IActionResult UploadCsv() =>
             View();

        [HttpPost]
        public async Task<IActionResult> UploadCsv(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                ViewBag.Message = "❌ Iltimos, CSV fayl yuklang.";
                return View();
            }

            string tableName = Path.GetFileNameWithoutExtension(file.FileName);
            string connStr = this.configuration.GetConnectionString("DefaultConnection");

            try
            {
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();

                using var reader = new StreamReader(file.OpenReadStream());
                bool firstLine = true;

                DataTable dt = new();
                int batchSize = 100000;
                int currentCount = 0;

                string? headerLine = await reader.ReadLineAsync();
                if (headerLine == null)
                {
                    ViewBag.Message = "❌ CSV fayl bo‘sh.";
                    return View();
                }
                var headers = headerLine.Split(',');

                foreach (var col in headers)
                {
                    string colName = col.Trim();
                    if (string.IsNullOrEmpty(colName))
                        colName = "Column" + dt.Columns.Count;
                    dt.Columns.Add(colName, typeof(string));
                }

                string dropIfExists = $@"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE [{tableName}]";
                using (var cmd = new SqlCommand(dropIfExists, conn))
                {
                    await cmd.ExecuteNonQueryAsync();
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

                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var values = line.Split(',');
                    dt.Rows.Add(values);
                    currentCount++;

                    if (currentCount >= batchSize)
                    {
                        await BulkInsertAsync(conn, dt, tableName);
                        dt.Clear();
                        currentCount = 0;
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
