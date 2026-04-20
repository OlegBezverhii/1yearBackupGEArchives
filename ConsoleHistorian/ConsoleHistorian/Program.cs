using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Proficy.Historian.ClientAccess.API;
using System.Configuration;


namespace ConsoleHistorian
{
    class Program
    {

        //static string _logPath = ConfigurationManager.AppSettings["HistorianServerName"];

        //где запустились, там и будем писать лог
        static string _logPath = AppDomain.CurrentDomain.BaseDirectory+"HistorianBackup.log";
        //локальное имя ПК будет использоваться для подключения
        static string _serverName = System.Environment.MachineName;
        //наименование хранилища, где будем смотреть архивы
        static string _datastorename = ConfigurationManager.AppSettings["DataStoreName"];
		//флаг необходимости делать бэкап во время remove
        static bool _needbackup = bool.Parse(ConfigurationManager.AppSettings["NeedBackup"] ?? "false");
        //ещё доп флаг - по идее remove должен был сделать бэкап сам, но не сделал
        static bool _backupinremove = bool.Parse(ConfigurationManager.AppSettings["BackupInRemove"] ?? "false");

        //константа на год Unix
        const int unixTimeYear = 1970;

        static void Main(string[] args)
        {
            Console.WriteLine("logPath = " + _logPath);

            //переменная для подключения к серверу Historian
            ServerConnection _sc;

            WriteLog("Подключаемся к серверу Historian.");
            Console.WriteLine("Подключаемся к серверу Historian.");

            try
            {
                _sc = new ServerConnection(new ConnectionProperties { ServerHostName = _serverName });
                _sc.Connect();

                //если подлючились, то 
                if (_sc.IsConnected())
                {
                    WriteLog("Подключение к серверу Historian - успешно!");
                    Console.WriteLine("Подключение к серверу Historian - успешно!");

                    WriteLog("Запрашиваем у хранилища " + _datastorename + " перечень архивов.");
                    Console.WriteLine("Запрашиваем у хранилища " + _datastorename + " перечень архивов.");

                    //Основной метод для работы с архивом
                    ProcessArchives(_sc, _datastorename);

                    //Если есть подключение и элемент, то отключаемся.
                    if (_sc != null && _sc.IsConnected()) _sc.Disconnect();
                    WriteLog("Подключение закрыто успешно.");

                }
                else
                {
                    WriteLog("Не удалось подключиться к серверу Historian!");
                    Console.WriteLine("Не удалось подключиться к серверу Historian!");
                    return;
                }


            }
            catch (Exception ex)
            {
                WriteLog("КРИТИЧЕСКАЯ ОШИБКА: " + ex.Message);
            }


            WriteLog("Программа остановлена.");
            Console.WriteLine("Программа остановлена.");

            //Для отладки
            //Console.ReadKey();
        }

        private static void ProcessArchives(ServerConnection sc, string datastorename)
        {
            ArchiveQueryParams queryParams = new ArchiveQueryParams { Namemask = "*" };

            //List<Archive> ArchiveList = new List<Archive>();
            List<Archive> ArchiveTemp;

            sc.IArchives.Query(ref queryParams, out ArchiveTemp, datastorename);

            var filteredArchives = ArchiveTemp
                .Where(a => a.StartTime.Year != unixTimeYear)
                .OrderBy(a => a.StartTime)
                .ToList();

            //Если ничего нет, то выходим
            if (filteredArchives.Count == 0) return;

            //Если самый новый архив, то используем .LastOrDefault()
            //Если нужен самый старый, то используем .FirstOrDefault();
            Archive oldestAcrhive = filteredArchives.FirstOrDefault(); //.OrderBy(item => item.StartTime)

            // 4. Логика: Если архив старше года - делаем Remove и удаляем файл.
            if (oldestAcrhive != null && oldestAcrhive.StartTime <= DateTime.Now.AddYears(-1))
            {
                WriteLog($"Обнаружен старый архив: {oldestAcrhive.Name} ({oldestAcrhive.StartTime})");
                Console.WriteLine($"Обнаружен старый архив: {oldestAcrhive.Name} ({oldestAcrhive.StartTime})");
				
				if (_needbackup == true)
				{
					WriteLog($"Флаг _needbackup в true, поэтому делаем дополнительный бэкап.");
					Console.WriteLine($"Флаг _needbackup в true, поэтому делаем дополнительный бэкап.");
					
					string ArchiveNameBackup = oldestAcrhive.Name.Replace(".iha", "_backup.zip");
					
					sc.IArchives.Backup(oldestAcrhive.Name, ArchiveNameBackup, _needbackup, oldestAcrhive.DataStoreName);
				}


                sc.IArchives.Remove(oldestAcrhive.Name, _backupinremove, oldestAcrhive.DataStoreName);
                WriteLog($"Архив {oldestAcrhive.Name} удален из Historian.");
                Console.WriteLine($"Архив {oldestAcrhive.Name} удален из Historian.");

                // Удаление файла
                try
                {
                    if (!IsFileInUse(oldestAcrhive.Filename))
                    {
                        File.Delete(oldestAcrhive.Filename);
                        WriteLog($"Файл {oldestAcrhive.Filename} удален с диска.");
                        Console.WriteLine($"Файл {oldestAcrhive.Filename} удален с диска.");
                    }
                    else
                    {
                        WriteLog($"ВНИМАНИЕ: Файл {oldestAcrhive.Filename} занят другим процессом, не удалось удалить.");
                        Console.WriteLine($"Файл {oldestAcrhive.Filename} удален с диска.");
                    }
                }
                catch (Exception ex)
                {
                    WriteLog($"Ошибка при удалении архива: {ex.Message}");
                }
            }
        }

        private static bool IsFileInUse(string filePath)
        {
            try
            {
                using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    return false;
                }
            }
            catch { return true; }
        }


        private static void WriteLog(string message)
        {
            try
            {
                string logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}{Environment.NewLine}";
                File.AppendAllText(_logPath, logEntry);
            }
            catch { /* Игнорируем ошибки записи лога, чтобы не вешать приложение */ }
        }
    }

}
