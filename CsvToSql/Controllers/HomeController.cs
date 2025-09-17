using Microsoft.AspNetCore.Mvc;

namespace CsvToSql.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()=>
             View();
    }
}
