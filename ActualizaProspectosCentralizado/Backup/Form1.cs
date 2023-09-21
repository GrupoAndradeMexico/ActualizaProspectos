using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Diagnostics;
using System.IO;
using System.Management;
using Microsoft.Win32;
using System.Data.SqlClient;
using System.Data;

namespace ActualizaProspectos
{
    public partial class Form1 : Form
    {
        /******************************************** ESTA ES LA VERSION QUE CORRE EN EL SERVIDOR ****************************          
         * NOTA: EL martes 11 de Noviembre SICOP dice que no debo mover el archivo de los prospectos.
         * Se modifica para que no se dispare el evento mediante fsw_created 
         * ahora se disparará el evento por fsw_changed
         * 
         * No se mueve el archivo a otra ruta, se COPIA a la nueva ruta como respaldo.
         * 
         * 
         * 
         * Centralizado: un archivo SICOP_PROSPECTOS_TEMP_DMS.txt será generado en una carpeta en específico, en el servidor, por SICOP 
         * para cada agencia, en consecuencia: Se realiza el cambio para que este programa cicle sobre todas las carpetas definidas en una tabla
         * y valide si hubo un cambio en el archivo SICOP_PROSPECTOS_TEMP_DMS.txt correspondiente, mediante el MD5 (se quita el fsw_changed).
         * 
         * si el archivo cambió, entonces se enviará a ejecución del ejecutable de BPro
         * 
         * SICOP agrega el ID PROSPECTO al nombre del archivo por lo que lo vuelve único, se quita el MD5.
         * 
         * 20150430 Se parametriza en BD la ruta del ejecutable, 
         * 
         */

               
        string ConnectionString = System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];        
        string SegundosEspera = System.Configuration.ConfigurationSettings.AppSettings["SegundosEspera"];
        string Latencia = System.Configuration.ConfigurationSettings.AppSettings["Latencia"];

        ConexionBD objDB = null; 


        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            FileInfo archivoExecutalbe = new FileInfo(Application.ExecutablePath.Trim());
            string NombreProceso = archivoExecutalbe.Name; 
            NombreProceso = NombreProceso.Replace(".exe", "");
            NombreProceso = NombreProceso.Replace(".EXE", "");
            if (CuentaInstancias(NombreProceso) == 1)
            {//la instancia debe ser igual a 1, que es esta misma instancia. Si es distinta entonces mandar el aviso de que ya se está ejecutand
                this.objDB = new ConexionBD(this.ConnectionString);
                this.timer1.Enabled = true;
                this.timer1.Interval = (Convert.ToInt16(Latencia) * 60000); 
                this.timer1.Start();
                Utilerias.WriteToLog("", "", Application.StartupPath + "Log.txt"); 
            }
            else {
                //Utilerias.WriteToLog("Ya existe una instancia de: " + NombreProceso + " se conserva la instancia actual", "Sincronizador_Load", Application.StartupPath + "\\Log.txt");
                Application.Exit();               
            }
        }


        private void RevisaCarpetas()
        {
            string Q = "Select * from SICOPMAQUINASPROSPECTOS where activo='True' order by Convert(int,numero_sucursal)";

            DataSet ds = this.objDB.Consulta(Q);
            foreach (DataRow lector in ds.Tables[0].Rows)
            {
                try
                {
                    string strusuario_bpro = lector["usuario_bpro"].ToString().Trim();
                    string strDominio = "";
                    string strbd_bpro = lector["bd_bpro"].ToString().Trim();
                    string strCarpetaLocalProspectos = lector["carpeta_local_prospectos"].ToString().Trim();
                    string strCarpetaServerDejar = lector["carpeta_server_dejar"].ToString().Trim();
                    string idmaquina = lector["id_maquina"].ToString().Trim();
                    string nombremaquina = lector["nombre"].ToString().Trim();
                    string id_agencia = lector["numero_sucursal"].ToString().Trim();
                    string PassLocal = lector["passw_local"].ToString().Trim();
                    string md5_actual = lector["md5_actual"].ToString().Trim();
                    string DirectorioDejaLogs = lector["carpeta_deja_logs"].ToString().Trim();
                    string mascara = lector["mascara"].ToString().Trim();
                    string RutaEjecutableBPro = lector["ruta_ejecutable_BPro"].ToString().Trim();
 

                    FileInfo[] Archivos = new DirectoryInfo(strCarpetaLocalProspectos).GetFiles(mascara);
                    foreach (FileInfo Archivo in Archivos)
                    {
                        if (FileReadyToRead(Archivo.FullName,1))
                        {
                            //Checksum chk = new Checksum();
                            //string md5 = chk.CalculateFileHash(Archivo.FullName, Algorithm.MD5);
                            //if (!md5.Equals(md5_actual.Trim()))
                            //{
                            ProcesaArchivo(Archivo.FullName, Archivo.Name, strCarpetaServerDejar, strusuario_bpro, strbd_bpro, strCarpetaLocalProspectos, DirectorioDejaLogs, RutaEjecutableBPro);
                                //Q = " Update SICOPMAQUINASPROSPECTOS set md5_actual='" + md5.Trim() + "' where id_maquina=" + idmaquina.Trim();
                                //if (this.objDB.EjecUnaInstruccion(Q) > 0)
                                //{
                                //    Utilerias.WriteToLog("Se actualizo el MD5 anterior: " + md5_actual.Trim() + " actual: " + md5.Trim() , "RevisaCarpetas", Application.StartupPath + "\\Log.txt"); 
                                //}
                            //}
                        }
                    }//del ciclo de cada Archivo
                }
                catch (Exception exw)
                {
                    Utilerias.WriteToLog(exw.Message, "RevisaCarpetas", Application.StartupPath + "\\Log.txt"); 
                }
            }//del ciclo de cada lector
        }




        #region procedimientos de soporte

        public string MataProceso(string NombreProceso)
        {
            string res = "";
            try
            {
                if (NombreProceso.Trim() != "")
                {
                    NombreProceso = NombreProceso.Replace(".exe", "");
                    NombreProceso = NombreProceso.Replace(".EXE", "");

                    Process[] localByName = Process.GetProcessesByName(NombreProceso);
                    foreach (Process proceso in localByName)
                    {
                        proceso.CloseMainWindow();
                        if (proceso.HasExited == false)
                        {
                            proceso.Kill();
                            proceso.Close();
                            res = "El proceso: " + NombreProceso + " ha sido eliminado del TaskManager";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
            return res;
        }

        /// <summary>
        /// Dada la ruta donde se encuentra un archivo ejecutable lanza su ejecucion
        /// </summary>
        /// <param name="rutaejecutable">El archivo ejecutable a ejecutar</param>
        /// <returns>Verdadero si pudo lanzar la ejecucion</returns>
        private bool LanzaEjecucion(string rutaejecutable)
        {
            bool res = false;
            try
            {

                //string filepath = @"C:\RepContavsNomina\Impersonate.bat";
                // Create the ProcessInfo object
                ProcessStartInfo psi = new ProcessStartInfo("cmd.exe");
                psi.UseShellExecute = false;
                psi.RedirectStandardOutput = true;
                psi.RedirectStandardInput = true;
                psi.RedirectStandardError = true;
                //impersonating
                //psi.UserName = "Administrator";
                //System.Security.SecureString psw = new SecureString();
                //foreach (Char ch in "Al3m4n14")
                //{
                //    psw.AppendChar(ch);
                //}
                //psi.Password = psw;
                //psi.Domain = System.Environment.MachineName;
                //psi.UseShellExecute = false;

                // Start the process           
                Process proc = Process.Start(psi);
                //StreamReader sr = File.OpenText(filepath);
                StreamWriter sw = proc.StandardInput;

                //while (sr.Peek() != -1)
                //{
                //    // Make sure to add Environment.NewLine for carriage return!
                //    sw.WriteLine(sr.ReadLine() + Environment.NewLine);
                //}
                sw.WriteLine(rutaejecutable + Environment.NewLine);

                //sr.Close();
                proc.Close();
                sw.Close();
                res = true;
            }
            catch (Exception ex)
            {
                Utilerias.WriteToLog(ex.Message, "LanzaEjecucion", Application.StartupPath + "\\Log.txt");
                Debug.WriteLine(ex.Message);
            }
            return res;
        }


        /// <summary>
        /// Consulta los procesos que se estan ejecutando en este momento 
        /// </summary>
        /// <param name="NombreProceso">A buscar si es que se está ejecutando</param>
        /// <returns>verdadero si el proceso está en ejecucion</returns>
        private bool EstaEnEjecucion(string NombreProceso)
        {
            bool res = false;
            try
            {
                if (NombreProceso.Trim() != "")
                {
                    NombreProceso = NombreProceso.Replace(".exe", "");
                    NombreProceso = NombreProceso.Replace(".EXE", "");

                    Process[] localByName = Process.GetProcessesByName(NombreProceso);
                    if (localByName.Length > 0)
                        res = true;
                    else
                        res = false;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }

            return res;
        }

        private bool FileReadyToRead(string filePath, int maxDuration)
        {
            int readAttempt = 0;
            while (readAttempt < maxDuration)
            {
                readAttempt++;
                try
                {
                    using (StreamReader stream = new StreamReader(filePath))
                    {
                        return true;
                    }
                }
                catch
                {
                    System.Threading.Thread.Sleep(60000);
                }
            }
            return false;
        }

        public string RespaldaLogs(string Ruta,string RutaRespaldo)
        {
            string res = "";
            try
            {
                FileInfo[] Archivos = new DirectoryInfo(Ruta).GetFiles("LOG*.txt");
                foreach (FileInfo Archivo in Archivos)
                {
                    Archivo.MoveTo(RutaRespaldo + "\\" + Archivo.Name);  
                    res="";
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                res = ex.Message;
            }
            return res;
        }

        #endregion


        private void ProcesaArchivo(string FullPath, string SoloNombreArchivo, string RutaDirectorioProcesados, string UsuarioBPRo, string BDBPRo, string DirectorioInspeccion, string DirectorioDejaLogs, string RutaEjecutableBPro)
        {
            try
            {
                RespaldaLogs(DirectorioDejaLogs, RutaDirectorioProcesados);
                
                    //"C:\Users\omorales\Desktop\Business Pro SICOP.exe" SICOP GMI GAZM_ZARAGOZA Importa C:\SiCoP\Importar\ SICOP_PROSPECTOS_TEMP_DMS.TXT 3N1CK3CD9DL259265 1000 1000
                    //"C:\Users\omorales\Desktop\Business Pro SICOP.exe" SICOP GMI GAZM_ZARAGOZA Importa C:\SiCoP\Importar\ SICOP_PROSPECTOS_TEMP_DMS.TXT 3N1CK3CD9DL259265

                    string fechahora = DateTime.Now.ToString("yyyyMMdd HH:mm:ss");                                                 
                         fechahora = fechahora.Replace(" ", "");
                         fechahora = fechahora.Replace(":", "");
                         fechahora = "_" + fechahora.Trim();
                                                
                        DateTime.Now.ToLongTimeString();    

                    const string quote = "\"";

                    string Sicop = "SICOP";
                    string Comando = quote + RutaEjecutableBPro.Trim() + quote + " {0} {1} {2} {3} {4} {5}";
                    //string Comando = this.RutaEjecutableBPro.Trim() + " {0} {1} {2} {3} {4} {5}";
                    string Sentido = "Importa";                    
                    string vin = "0";

                    Comando = string.Format(Comando, Sicop, UsuarioBPRo, BDBPRo, Sentido, DirectorioInspeccion.Trim(), SoloNombreArchivo.Trim(), vin.Trim());

                    if (LanzaEjecucion(Comando))
                    {
                        Utilerias.WriteToLog("Se ejecutó el comando: " + Comando, "ProcesaArchivo", Application.StartupPath + "\\Log.txt");
                        System.Threading.Thread.Sleep(Convert.ToInt16(this.SegundosEspera) * 1000);
                        
                        SoloNombreArchivo = SoloNombreArchivo.Replace(".txt", fechahora + ".txt");

                        if (File.Exists(RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim()))
                            File.Delete(RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim());

                        File.Copy(FullPath.Trim(), RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim());                        
                        //Utilerias.WriteToLog("Se Movio el archivo: " + RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim() + " para respaldo.", "ProcesaArchivo", Application.StartupPath + "\\Log.txt");
                        File.Delete(FullPath.Trim());                                                 
                    }                
            }
            catch (Exception ex)
            {
                Utilerias.WriteToLog(ex.Message, "ProcesaArchivo", Application.StartupPath + "\\Log.txt");
            }                
        }
        

        private void Form1_Paint(object sender, PaintEventArgs e)
        {
            this.ntiBalloon.Icon = this.Icon;
            this.ntiBalloon.Text = "SICOP REGISTRO DE PROSPECTOS";
            this.ntiBalloon.Visible = true;
            this.ntiBalloon.ShowBalloonTip(1, "SICOP REGISTRO DE PROSPECTOS", " En espera de instrucciones ", ToolTipIcon.Info);
            this.Hide();
            this.Visible = false;
        }

        //le debe llegar sin la extension .exe
        private int CuentaInstancias(string NombreProceso)
        {
            int res = 0;
            try
            {
                if (NombreProceso.Trim() != "")
                {
                    Process[] localByName = Process.GetProcessesByName(NombreProceso);
                    res = localByName.Length;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }

            return res;
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            this.ntiBalloon.ShowBalloonTip(1, "SICOP REGISTRO DE PROSPECTOS", " Revisando carpetas ", ToolTipIcon.Info);
            RevisaCarpetas();
        }

        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            this.ntiBalloon.Visible = false;
            this.ntiBalloon = null;
        }               
    }
}
