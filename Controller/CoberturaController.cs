using ConsoleApi.Model;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;


namespace MyRestApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CoberturaController : ControllerBase
    {
        private static readonly string connectionString = "Data Source=localhost;Initial Catalog=Prueba-VDHMIL;User ID=sa;Password=&ecurity23;";
        private static readonly string connectionStringProd = "Data Source=HMIL-SPC-APPPRD;Initial Catalog=RRHH;User ID=sa;Password=P@$$W0RD;";

        private static SqlConnection sqlConnection = null;
        private static string rutaDirectorio = @"/home/haki/csv/"; // Ruta donde están tus archivos CSV

        public CoberturaController()
        {
            sqlConnection = new SqlConnection(connectionString);
        }

        // GET: api/<cobertura>
        [HttpGet]
        public IEnumerable<dynamic> Get()
        {
            using (var connection = new ConexionManager(connectionString).GetConnection())
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT * FROM dbo.[MAESTRO DE DPC]"; // Remove "@" before "SELECT"
                var dataReader = cmd.ExecuteReader();
                List<Persona> ListaPersonas = new List<Persona>();

                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        Persona persona = new Persona
                        {
                            ID = dataReader.GetInt32(dataReader.GetOrdinal("ID")),
                            ORIGEN_1 = dataReader.GetString(dataReader.GetOrdinal("ORIGEN_1")),
                            ORD = dataReader.GetInt32(dataReader.GetOrdinal("ORD")),
                            IDENTIDAD = dataReader.GetString(dataReader.GetOrdinal("IDENTIDAD")),
                            NOMBRE1 = dataReader.GetString(dataReader.GetOrdinal("NOMBRE1")),
                            NOMBRE2 = dataReader.GetString(dataReader.GetOrdinal("NOMBRE2")),
                            APELLIDO1 = dataReader.GetString(dataReader.GetOrdinal("APELLIDO1")),
                            APELLIDO2 = dataReader.GetString(dataReader.GetOrdinal("APELLIDO2")),
                            SEXO = dataReader.GetString(dataReader.GetOrdinal("SEXO")),
                            F_NAC = dataReader.IsDBNull(dataReader.GetOrdinal("F_NAC")) ? (DateTime?)null : dataReader.GetDateTime(dataReader.GetOrdinal("F_NAC")),
                            GRADO = dataReader.GetString(dataReader.GetOrdinal("GRADO")),
                            F_ING = dataReader.IsDBNull(dataReader.GetOrdinal("F_ING")) ? (DateTime?)null : dataReader.GetDateTime(dataReader.GetOrdinal("F_ING")),
                            TIPO = dataReader.GetString(dataReader.GetOrdinal("TIPO")),
                            CARGA_I = dataReader.GetString(dataReader.GetOrdinal("CARGA_I")),
                            ORIGEN = dataReader.GetString(dataReader.GetOrdinal("ORIGEN")),
                            UM = dataReader.GetString(dataReader.GetOrdinal("UM")),
                            CARGO = dataReader.GetString(dataReader.GetOrdinal("CARGO")),
                            N_EXPEDIENTE = dataReader.GetString(dataReader.GetOrdinal("N_EXPEDIENTE")),
                            CLASIFICACION = dataReader.GetString(dataReader.GetOrdinal("CLASIFICACION")),
                            CEDULA = dataReader.GetString(dataReader.GetOrdinal("CEDULA")),
                            NO_CARNE = dataReader.GetString(dataReader.GetOrdinal("NO_CARNE")),
                            fecha_ingreso_BD = dataReader.GetDateTime(dataReader.GetOrdinal("fecha_ingreso_BD"))
                        };
                        ListaPersonas.Add(persona);
                    }
                }

                return ListaPersonas;
            }
        }

        // POST: api/<cobertura>
        [HttpPost]
        public IActionResult BulkInsertFromAccess()
        {
            var connection = new ConexionManager(connectionString).GetConnection();

            if (connection != null)
            {
                try
                {
                    Stopwatch totalStopwatch = Stopwatch.StartNew(); // Iniciar el cronómetro total

                    ProcesarArchivosCSV(rutaDirectorio);

                    totalStopwatch.Stop(); // Detener el cronómetro total
                    Console.WriteLine($"Tiempo total de procesamiento: {totalStopwatch.Elapsed}");

                    connection.Close();

                    // Devolver una respuesta exitosa (código 200)
                    return Ok("Bulk insert desde Access a SQL Server completado exitosamente.");
                }
                catch (Exception ex)
                {
                    // En caso de error, devolver un código de error (por ejemplo, 500) con el mensaje de error
                    return StatusCode(500, $"Error: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("No se pudo abrir la conexión con la base de datos.");

                // En caso de que la conexión falle, también devolver un código de error (por ejemplo, 500)
                return StatusCode(500, "Error al abrir la conexión con la base de datos.");
            }
        }

        public void ProcesarArchivosCSV(string rutaDirectorio)
        {
            try
            {
                string[] archivosCSV = Directory.GetFiles(rutaDirectorio, "*.csv");
                foreach (string archivo in archivosCSV)
                {
                    Stopwatch archivoStopwatch = Stopwatch.StartNew(); // Iniciar el cronómetro para el archivo actual

                    using (var lector = new StreamReader(archivo))
                    {
                        // Crear un DataTable para almacenar los datos
                        DataTable dataTable = new DataTable();

                        // Define el diccionario de mapeo de columnas
                        Dictionary<string, string> columnMapping = new Dictionary<string, string>()
                        {
                            { "Origen", "ORIGEN_1" },
                            { "Ord", "ORD" },
                            { "Identidad", "IDENTIDAD" },
                            { "Expediente", "N_EXPEDIENTE" },
                            { "Cedula", "CEDULA" },
                            { "Grado", "GRADO" },
                            { "Dgrado", "DGRADO" },
                            { "P_Apellido", "APELLIDO1" },
                            { "S_apellido", "APELLIDO2" },
                            { "P_nombre", "NOMBRE1" },
                            { "S_nombre", "NOMBRE2" },
                            { "Sexo", "SEXO" },
                            { "Fecha_Nac", "F_NAC" },
                            { "Fecha_ing", "F_ING" },
                            { "Dcargo", "CARGO" },
                            { "Publico", "PUBLICO" },
                            { "Dunidad", "UM" },
                            { "Fecha_Ret", "FECHA_RET" },
                            { "Inico_Cob", "INICIO_COB" },
                            { "Fin_Cob", "FIN_COB" },
                            { "Mes_Proceso", "MES_PROCESO" },
                            { "Estado", "ESTADO" },
                            { "NCarnet", "NCARNET" },
                            { "LugarChequeo", "LUGAR_CHEQUEO" }
                        };


                        // Agregar las columnas en el orden correcto
                        foreach (var entry in columnMapping)
                        {
                            string columnNameInDB = entry.Value;

                            // Agregar la columna con el nombre correspondiente de la base de datos
                            dataTable.Columns.Add(columnNameInDB);
                        }

                        // Leer y agregar los datos al nuevo DataTable, empezando desde la segunda fila
                        bool primeraFila = true;
                        int numColumnas = columnMapping.Count;
                        string[] columnasOrdenadas = new string[numColumnas]; // Arreglo para almacenar las columnas en orden
                        while (!lector.EndOfStream)
                        {
                            string linea = lector.ReadLine();

                            // Saltar la primera fila que contiene los nombres de las columnas
                            if (primeraFila)
                            {
                                // Dividir la línea para obtener las columnas
                                string[] columnasCSV = linea.Split(',');

                                // Reordenar las columnas según el mapeo y almacenarlas en el arreglo
                                for (int i = 0; i < numColumnas; i++)
                                {
                                    string columnNameInCSV = columnMapping.ElementAt(i).Key;
                                    string columnNameInBD = columnMapping.ElementAt(i).Value;
                                    int index = Array.IndexOf(columnasCSV, columnNameInCSV);
                                    if (index != -1)
                                    {
                                        columnasOrdenadas[index] = columnNameInBD;
                                    }
                                    else
                                    {
                                        // Si la columna no se encuentra en el archivo CSV, establecer un valor predeterminado
                                        columnasOrdenadas[i] = ""; // O cualquier otro valor predeterminado que desees
                                    }
                                }

                                primeraFila = false;
                                continue;
                            }

                            string[] valores = linea.Split(','); // Separar por comas

                            // Crear un nuevo array para almacenar los valores en el orden correcto
                            object[] valoresOrdenados = new object[numColumnas];
                            List<Grado> ListaGradosMilitares = ObtenerGradosMilitares("S1");

                            // Mapear los valores de la fila actual al nuevo array según el arreglo de columnas ordenadas
                            for (int i = 0; i < numColumnas; i++)
                            {
                                if (!string.IsNullOrEmpty(columnasOrdenadas[i]))
                                {
                                    int index = Array.IndexOf(columnasOrdenadas, columnasOrdenadas[i]);
                                    valoresOrdenados[index] = valores[index];
                                }
                            }

                            // Agregar la nueva fila al nuevo DataTable
                            dataTable.Rows.Add(valoresOrdenados);
                        }

                        using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                        {
                            sqlConnection.Open();

                            // Iniciar la transacción para el bulk insert
                            SqlTransaction transaction = sqlConnection.BeginTransaction();

                            // Crear el objeto SqlBulkCopy
                            using (var bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction))
                            {
                                bulkCopy.DestinationTableName = "dbo.[MAESTRO DE DPC]"; // Reemplaza con el nombre de tu tabla
                                bulkCopy.BatchSize = 10000; // Tamaño del lote a insertar
                                bulkCopy.BulkCopyTimeout = 600; // Tiempo de espera en segundos

                                try
                                {
                                    // Realizar el bulk insert
                                    bulkCopy.WriteToServer(dataTable);

                                    // Commit de la transacción si todo fue exitoso
                                    transaction.Commit();
                                    Console.WriteLine("Bulk insert exitoso para el archivo: " + archivo);
                                }
                                catch (Exception ex)
                                {
                                    // Rollback en caso de error
                                    Console.WriteLine("Error durante el bulk insert para el archivo " + archivo + ": " + ex.Message);
                                    transaction.Rollback();
                                }
                            }
                        }
                    }

                    // Pequeña pausa para permitir que el sistema libere el archivo
                    System.Threading.Thread.Sleep(100);

                    archivoStopwatch.Stop(); // Detener el cronómetro para el archivo actual
                    Console.WriteLine($"Tiempo de procesamiento para {archivo}: {archivoStopwatch.Elapsed}");
                }
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine($"Error: El directorio {rutaDirectorio} no existe");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }


        public List<Grado> ObtenerGradosMilitares(string codigo = null)
        {
            try
            {
                List<Grado> ListaGradosMilitares = new List<Grado>();
                using (var connection = new ConexionManager(connectionStringProd).GetConnection())
                {

                    var cmd = connection.CreateCommand();
                    cmd.CommandText = "SELECT * FROM dbo.[CAT DE GRADOS]"; // Remove "@" before "SELECT"
                    var dataReader = cmd.ExecuteReader();

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            Grado grado = new Grado
                            {
                                CODIGO = dataReader.GetString(dataReader.GetOrdinal("CODIGO")),
                                DESCRIPCION = dataReader.GetString(dataReader.GetOrdinal("DESCRIPCION"))
                            };
                            ListaGradosMilitares.Add(grado);
                        }
                    }

                }

                if(!string.IsNullOrEmpty(codigo)){
                    return ListaGradosMilitares.Where(grado => grado.CODIGO == codigo).ToList();
                }

                return ListaGradosMilitares;
            }
            catch (Exception ex)
            {
                // Handle any exceptions here
                Console.WriteLine("Error al obtener grados militares: " + ex.Message);
                // Return an empty array or null indicating failure
                return new List<Grado>();
            }
        }


    }
}
