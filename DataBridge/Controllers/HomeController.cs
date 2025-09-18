using Microsoft.AspNetCore.Mvc;

namespace DataBridge.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()=>
             View();
    }
}
