using Convey;
using Convey.HTTP;
using Convey.Metrics.Prometheus;
using Convey.Tracing.Jaeger;
using Convey.Types;
using Convey.WebApi;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Trill.Saga.Logging;

namespace Trill.Saga
{
    internal class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddConvey().AddWebApi().AddCore().Build();
            services.AddSingleton<ICorrelationIdFactory, CorrelationIdFactory>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseMiddleware<LogContextMiddleware>();
            app.UseJaeger();
            app.UsePrometheus();
            app.UseRouting();
            app.UseCore();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync(context.RequestServices.GetService<AppOptions>().Name);
                });
            });
        }
    }
}
