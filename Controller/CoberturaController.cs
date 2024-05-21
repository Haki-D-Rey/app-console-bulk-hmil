using ConsoleApi.Model;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;

namespace MyRestApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CoberturaController : ControllerBase
    {
        private static readonly string connectionString = "Data Source=localhost;Initial Catalog=Prueba-VDHMIL;User ID=sa;Password=&ecurity23;";
        private readonly SqlConnection sqlConnection;

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

            string accessDatabasePath = @"~\2024.04-HOSPITAL"; // Ruta completa de tu archivo de base de datos de Access
            string accessPassword = "LADA2107"; // La contraseña de tu archivo .accdb

            using (OleDbConnection accessConnection = new OleDbConnection($"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={accessDatabasePath};Jet OLEDB:Database Password={accessPassword}"))
            {
                accessConnection.Open();
                OleDbCommand cmd = new OleDbCommand("SELECT * FROM Cobertura_Hospital", accessConnection);
                OleDbDataReader reader = cmd.ExecuteReader();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    bulkCopy.DestinationTableName = "dbo.[MAESTRO DE DPC]";
                    bulkCopy.BatchSize = 10000; // Tamaño del lote a insertar
                    bulkCopy.BulkCopyTimeout = 600; // Tiempo de espera en segundos
                    try
                    {
                        bulkCopy.WriteToServer(reader);
                        return Ok("Bulk insert from Access to SQL Server completed successfully.");
                    }
                    catch (Exception ex)
                    {
                        return StatusCode(500, $"Error: {ex.Message}");
                    }
                }
            }
        }
    }
}
