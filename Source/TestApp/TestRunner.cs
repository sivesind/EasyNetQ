using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Threading.Tasks;
using EasyNetQ;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;

namespace TestRabbitMQCluster
{
    public class TestRunner
    {
        private ILog _logger;

        public void Run(string[] args)
        {
            //LSP 2.0 prod
            //var connectionString =
            //    "host=192.168.185.19;username=test_slettes;password=test;persistentMessages=false;timeout=5;publisherConfirms=true";

            //mitt cluster
            var connectionString =
                "host=larse3,les-laptop;username=transx;password=transx;persistentMessages=false;timeout=5;publisherConfirms=true";

            //Maskin i DMZ
            //var connectionString =
            //    "host=192.168.7.7;username=transx;password=transx;persistentMessages=false;timeout=5;publisherConfirms=true";
            var flags = new Flags(args);
            ConfigureLogging(flags);

            long sentCount = 0;
            long receiveCount = 0;
            string serviceName = "RabbitMQ";
            TimeSpan serviceStartStopTimeout = TimeSpan.FromSeconds(10);
            Task subsciberTask;
            Task publisherTask;
            Task rabbitControlTask;

            var myName = Environment.MachineName;

            if (!flags.Have("rab"))
            {
                Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        Task.Delay(5000).Wait();
                        _logger.Info($"Sent: {sentCount} Received: {receiveCount} Diff: {sentCount - receiveCount}");
                    }
                });
            }
            else
            {
                string modMinute = flags.Following("modminute");
                string periodic = flags.Following("periodic");

                _logger.Info($"Running in rabbitmode {nameof(modMinute)}: {modMinute} {nameof(periodic)}: {periodic}");

                rabbitControlTask = Task.Factory.StartNew(() =>
                {
                    DateTimeOffset timeForLastRestart = DateTimeOffset.Now;

                    while (true)
                    {
                        if (modMinute != null && DateTimeOffset.Now.Minute.ToString().EndsWith(modMinute)
                            ||
                            periodic != null &&
                            DateTimeOffset.Now - timeForLastRestart > TimeSpan.FromSeconds(double.Parse(periodic)))
                        {
                            var serviceController = new ServiceController(serviceName);
                            try
                            {
                                if (serviceController.Status == ServiceControllerStatus.Running)
                                {
                                    _logger.Info("Stopping rabbit");
                                    serviceController.Stop();
                                    serviceController.WaitForStatus(ServiceControllerStatus.Stopped,
                                        serviceStartStopTimeout);
                                    _logger.Info("Stopped rabbit");
                                    //leave down for a while, to let clients switch broker
                                    Task.Delay(TimeSpan.FromSeconds(10)).Wait();
                                }
                                _logger.Info("Starting rabbit");
                                serviceController.Start();
                                serviceController.WaitForStatus(ServiceControllerStatus.Running, serviceStartStopTimeout);
                                _logger.Info("Started rabbit");

                                timeForLastRestart = DateTimeOffset.Now;

                                //wait at least till next minute in modminute mode
                                if (modMinute != null)
                                {
                                    Task.Delay(TimeSpan.FromSeconds(51)).Wait();
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Info($"Error stopping rabbit: {e}");
                            }
                        }
                        Task.Delay(TimeSpan.FromSeconds(1)).Wait();
                    }
                });
            }

            //sub
            if (flags.Have("sub"))
            {
                subsciberTask = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        var bus = RabbitHutch.CreateBus(connectionString,
                            x => x.Register<IEasyNetQLogger>(_ => new RabbitLog4NetLogger(_logger)));
                        while (true)
                        {
                            try
                            {
                                //connect once, on first connection to broker
                                if (bus.IsConnected)
                                {
                                    bus.Subscribe<Msg>("TestRabbitMQCluster", msg =>
                                    {
                                        receiveCount++;
                                        _logger.Info($"Received: {msg.Text}");
                                    });
                                    break;
                                }
                                Task.Delay(100).Wait();
                            }
                            catch
                            {
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Error($"Error connecting to bus for subscibe, exiting task: {e}");
                    }
                });
            }

            //pub
            if (flags.Have("pub"))
            {
                publisherTask = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        var bus = RabbitHutch.CreateBus(connectionString,
                            x => x.Register<IEasyNetQLogger>(_ => new RabbitLog4NetLogger(_logger)));
                        while (true)
                        {
                            try
                            {
                                if (bus.IsConnected)
                                {
                                    bus.Publish(new Msg($"{myName}: msg#{sentCount}"));
                                    sentCount++;
                                    _logger.Info($"{myName} sent msg#{sentCount}");
                                }
                                else
                                {
                                    _logger.Info("Did not try to publish, bus reports not connected");
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Info($"Exception on publish: {e}");
                            }
                            Task.Delay(200).Wait();
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Error($"Error connecting to bus for publish, exiting task: {e}");
                    }
                }, TaskCreationOptions.LongRunning);
            }

            Console.ReadKey();
        }

        private void ConfigureLogging(Flags flags)
        {
            string loggerName = $"{Environment.MachineName}_";
            loggerName += flags.Have("rab") ? "rabbitcontrol" : "pubsub";
            _logger = LogManager.GetLogger(loggerName);
            var layout = new PatternLayout("%date [%thread] %-5level - %message%newline");

            //file
            var rollingFileAppender = new RollingFileAppender
            {
                File = $"log/{loggerName}.log",
                Layout = layout,
                Name = loggerName,
                AppendToFile = false
            };

            layout.ActivateOptions();
            rollingFileAppender.ActivateOptions();
            BasicConfigurator.Configure(rollingFileAppender);

            //console
            var coloredConsoleAppender = new ColoredConsoleAppender
            {
                Threshold = Level.Info,
                Layout = layout
            };

            coloredConsoleAppender.AddMapping(new ColoredConsoleAppender.LevelColors
            {
                Level = Level.Debug,
                ForeColor = ColoredConsoleAppender.Colors.Cyan
                            | ColoredConsoleAppender.Colors.HighIntensity
            });
            coloredConsoleAppender.AddMapping(new ColoredConsoleAppender.LevelColors
            {
                Level = Level.Info,
                ForeColor = ColoredConsoleAppender.Colors.Green
                            | ColoredConsoleAppender.Colors.HighIntensity
            });
            coloredConsoleAppender.AddMapping(new ColoredConsoleAppender.LevelColors
            {
                Level = Level.Warn,
                ForeColor = ColoredConsoleAppender.Colors.Purple
                            | ColoredConsoleAppender.Colors.HighIntensity
            });
            coloredConsoleAppender.AddMapping(new ColoredConsoleAppender.LevelColors
            {
                Level = Level.Error,
                ForeColor = ColoredConsoleAppender.Colors.Red
                            | ColoredConsoleAppender.Colors.HighIntensity
            });
            coloredConsoleAppender.AddMapping(new ColoredConsoleAppender.LevelColors
            {
                Level = Level.Fatal,
                ForeColor = ColoredConsoleAppender.Colors.White
                            | ColoredConsoleAppender.Colors.HighIntensity,
                BackColor = ColoredConsoleAppender.Colors.Red
            });

            coloredConsoleAppender.ActivateOptions();
            Hierarchy hierarchy =
                (Hierarchy) LogManager.GetRepository();
            hierarchy.Root.AddAppender(coloredConsoleAppender);
        }


        public class Flags
        {
            private readonly List<string> _args;

            public Flags(string[] args)
            {
                _args = args.ToList();
            }

            public bool Have(string flag)
            {
                return _args.Contains(flag);
            }

            public string Following(string flag)
            {
                if (!_args.Contains(flag))
                {
                    return null;
                }
                return _args[_args.IndexOf(flag) + 1];
            }
        }

        public class Msg
        {
            public Msg(string text)
            {
                Text = text;
            }

            public string Text;
        }
    }

    public class RabbitLog4NetLogger : IEasyNetQLogger
    {
        private readonly ILog _logger;

        public RabbitLog4NetLogger(ILog logger)
        {
            _logger = logger;
        }

        public void DebugWrite(string format, params object[] args)
        {
            _logger.DebugFormat(format, args);
        }

        public void InfoWrite(string format, params object[] args)
        {
            _logger.InfoFormat(format, args);
        }

        public void ErrorWrite(string format, params object[] args)
        {
            _logger.ErrorFormat(format, args);
        }

        public void ErrorWrite(Exception exception)
        {
            _logger.Error($"Exception: {exception}");
        }
    }
}