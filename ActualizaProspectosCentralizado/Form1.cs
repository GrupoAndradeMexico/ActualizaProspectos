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
using System.IO;
using System.Collections;
using System.Threading;

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
         * 20160308 Para que no se cicle y trabe todas las agencias, solo permanecerá "trabada" la instancia del ejecutable que no funcione.
         * 20160718 Se agrega soporte para el sensor de Prospectos.
         * 20160909 Se agrega rfc al sensor de Prospectos
         * 20200628 Se agrega código para prospectos seminuevos.
         * 20200702 Se agrega consulta a GA_Corporativa..cat_idsicop para saber si ya le guardó el SICOP en la tabla normalizada
         */

        #region Comentarios de Actualizacion.
        // 20181218 Se solicita escribir fisicamente en la tabla:  
        //[192.168.20.29].GA_Corporativa.dbo.Per_Personas
        // en lugar de la tabla :  PER_PERSONAS y/o la vista en la base de datos operativa,
        // esto obliga a diferenciar si la Base está centralizada o no.

        //20220830 Se agrega parametrizar la consulta por agencia y o grupo de agencias.

        #endregion



        string ConnectionString = System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];        
        string SegundosEspera = System.Configuration.ConfigurationSettings.AppSettings["SegundosEspera"];
        string Latencia = System.Configuration.ConfigurationSettings.AppSettings["Latencia"];

        string ConnStringPerPersonasCentralizada = System.Configuration.ConfigurationSettings.AppSettings["ConnStringPerPersonasCentralizada"];
        string IP_TablaPerPersonasCentralizada   = System.Configuration.ConfigurationSettings.AppSettings["IP_TablaPerPersonasCentralizada"];
        string BD_TablaPerPersonasCentralizada   = System.Configuration.ConfigurationSettings.AppSettings["BD_TablaPerPersonasCentralizada"];  //"GA_Corporativa" />
        string TablaPerPersonasCentralizada      = System.Configuration.ConfigurationSettings.AppSettings["TablaPerPersonasCentralizada"];   //" value ="Per_Personas"/>  
        string Modo                              = System.Configuration.ConfigurationSettings.AppSettings["Modo"];

        string id_agencia_in                     = System.Configuration.ConfigurationSettings.AppSettings["id_agencia_in"]; //para poder clonar el exe y dedicar un solo exe para una sola agencia o grupo de agencias.
        string id_agencia_not_in                 = System.Configuration.ConfigurationSettings.AppSettings["id_agencia_not_in"]; //para que excluya este grupo de agencias de la consulta.
        string LogFile                           = System.Configuration.ConfigurationSettings.AppSettings["LogFile"]; 

        Dictionary<string, string> dicProspectoNombre = new Dictionary<string, string>();

        ConexionBD objDB = null;

        string agenciaCentralizada = ""; //20181218 para discernir si se usa la vista PER_PERSONAS ó update [192.168.20.29].GA_Corporativa.dbo.Per_Personas set PER_SICOP = '171054012510998' where  PER_IDPERSONA = 143012


        Thread hilogenericolocal = null;

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

            //string RFC = ObtenDeArchivo(@"C:\AndradeGPO\ActualizarCampoEnBP\ActualizaProspectos\ActualizaProspectosCentralizado\bin\Debug\SICOP_PROSPECTOS_TEMP_DMS_172870009512997_20160727114040.txt", "RFC");
  
            if (CuentaInstancias(NombreProceso) == 1)
            {//la instancia debe ser igual a 1, que es esta misma instancia. Si es distinta entonces mandar el aviso de que ya se está ejecutand
                this.objDB = new ConexionBD(this.ConnectionString);
                this.timer1.Enabled = true;
                this.timer1.Interval = (Convert.ToInt16(Latencia) * 60000); 
                this.timer1.Start();
                Utilerias.WriteToLog("", "", Application.StartupPath + "\\" + LogFile.Trim());                
            }
            else {
                //Utilerias.WriteToLog("Ya existe una instancia de: " + NombreProceso + " se conserva la instancia actual", "Sincronizador_Load", Application.StartupPath + "\\Log.txt");
                Application.Exit();               
            }
        }


        private void RevisaCarpetas()
        {
            string Q = "Select * from SICOPMAQUINASPROSPECTOS where activo='True' ";
           // Q += " and numero_sucursal='49'"; //Por comentariar           
            if (this.id_agencia_in.Trim() != "")
            {
                Q += " and numero_sucursal in " + this.id_agencia_in.Trim(); // (11,12,35)
            }
            if (id_agencia_not_in.Trim() != "")
            {
                Q += " and numero_sucursal not in " + this.id_agencia_not_in.Trim(); // (11,12,35)
            }
            Q += " order by Convert(int,numero_sucursal)";

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

                    #region Nuevos
                    FileInfo[] Archivos = new DirectoryInfo(strCarpetaLocalProspectos).GetFiles(mascara);
                    foreach (FileInfo Archivo in Archivos)
                    {
                        if (FileReadyToRead(Archivo.FullName,1))
                        {
                            //20160718 Antes de procesar el archivo del prospecto, consultamos su estatus y escribimos en un log.
                            string fecha_archivo = DateTime.Now.ToLongDateString() + " " + DateTime.Now.ToString("HH:mm:ss"); ; 
                            string PerSICOPenArchivo = ObtenDeArchivo(Archivo.FullName, "C_Clave");
                            string Nombre_Pila = ObtenDeArchivo(Archivo.FullName, "Nombre_Pila");
                            string Apellido_Materno = ObtenDeArchivo(Archivo.FullName, "Apellido_Materno");
                            string Apellido_Paterno = ObtenDeArchivo(Archivo.FullName, "Apellido_Paterno");
                            string RFCenArchivo = ObtenDeArchivo(Archivo.FullName, "RFC"); //20160909 se agrega el rfc con que se envió.
                            string NombreEnArchivo = Nombre_Pila.Trim() + " " + Apellido_Paterno.Trim() + " " + Apellido_Materno.Trim();  //20160815  ObtenDeArchivo(Archivo.FullName, "NombreFac");
                            string yaexiste = EscribeLogProspecto(PerSICOPenArchivo, NombreEnArchivo, RFCenArchivo, id_agencia, fecha_archivo,"Nuevos");
                            string idpersonaenFax = ObtenDeArchivo(Archivo.FullName, "Telefono2_Trabajo");

                           // if (idpersonaenFax != "")
                           // {
                           //     idpersonaenFax = idpersonaenFax.TrimStart('0'); //quitamos los ceros a la izquierda...
                           //     ForzaMatchEnBpro(id_agencia, idpersonaenFax, PerSICOPenArchivo);  
                           // }


                            ProcesaArchivo(Archivo.FullName, Archivo.Name, strCarpetaServerDejar, strusuario_bpro, strbd_bpro, strCarpetaLocalProspectos, DirectorioDejaLogs, RutaEjecutableBPro);

                            if (yaexiste.Trim() == "") //El problema es que BPRo No registra de inmediato y no es sincrono.
                            { //Por tanto lo inscribimos para que en el siguiente ciclo de reloj lo consulte
                                if (!this.dicProspectoNombre.ContainsKey(PerSICOPenArchivo))
                                    this.dicProspectoNombre.Add(PerSICOPenArchivo, NombreEnArchivo + "|" + id_agencia + "|0|" + fecha_archivo + "|" + RFCenArchivo + "|Nuevos");
                                    //EscribeLogProspecto(PerSICOPenArchivo, NombreEnArchivo, id_agencia);
                            }
                        }
                    }//del ciclo de cada Archivo
                    #endregion //Nuevos.


                    #region SEMINUEVOS 20200628
                    //asi esta declarada: C:\LB\SICOP\Sicop\FUERZA\Prospectos\
                    strCarpetaLocalProspectos = strCarpetaLocalProspectos.Replace("Prospectos\\","SEMINUEVOS\\Prospectos\\"); 
                    FileInfo[] ArchivosSemi = new DirectoryInfo(strCarpetaLocalProspectos).GetFiles(mascara);
                    foreach (FileInfo Archivo in ArchivosSemi)
                    {
                        if (FileReadyToRead(Archivo.FullName, 1))
                        {
                            //20160718 Antes de procesar el archivo del prospecto, consultamos su estatus y escribimos en un log.
                            string fecha_archivo = DateTime.Now.ToLongDateString() + " " + DateTime.Now.ToString("HH:mm:ss"); ;
                            string PerSICOPenArchivo = ObtenDeArchivo(Archivo.FullName, "C_Clave");
                            string Nombre_Pila = ObtenDeArchivo(Archivo.FullName, "Nombre_Pila");
                            string Apellido_Materno = ObtenDeArchivo(Archivo.FullName, "Apellido_Materno");
                            string Apellido_Paterno = ObtenDeArchivo(Archivo.FullName, "Apellido_Paterno");
                            string RFCenArchivo = ObtenDeArchivo(Archivo.FullName, "RFC"); //20160909 se agrega el rfc con que se envió.
                            string NombreEnArchivo = Nombre_Pila.Trim() + " " + Apellido_Paterno.Trim() + " " + Apellido_Materno.Trim();  //20160815  ObtenDeArchivo(Archivo.FullName, "NombreFac");
                            string yaexiste = EscribeLogProspecto(PerSICOPenArchivo, NombreEnArchivo, RFCenArchivo, id_agencia, fecha_archivo,"Seminuevos");
                            string idpersonaenFax = ObtenDeArchivo(Archivo.FullName, "Telefono2_Trabajo");

                           // if (idpersonaenFax != "")
                           // {
                           //     idpersonaenFax = idpersonaenFax.TrimStart('0'); //quitamos los ceros a la izquierda...
                           //     ForzaMatchEnBpro(id_agencia, idpersonaenFax, PerSICOPenArchivo);
                           // }


                            ProcesaArchivo(Archivo.FullName, Archivo.Name, strCarpetaServerDejar, strusuario_bpro, strbd_bpro, strCarpetaLocalProspectos, DirectorioDejaLogs, RutaEjecutableBPro);

                            if (yaexiste.Trim() == "") //El problema es que BPRo No registra de inmediato y no es sincrono.
                            { //Por tanto lo inscribimos para que en el siguiente ciclo de reloj lo consulte
                                if (!this.dicProspectoNombre.ContainsKey(PerSICOPenArchivo))
                                    this.dicProspectoNombre.Add(PerSICOPenArchivo, NombreEnArchivo + "|" + id_agencia + "|0|" + fecha_archivo + "|" + RFCenArchivo + "|Seminuevos");
                                //EscribeLogProspecto(PerSICOPenArchivo, NombreEnArchivo, id_agencia);
                            }
                        }
                    }//del ciclo de cada Archivo
                    #endregion

                }
                catch (Exception exw)
                {
                    Utilerias.WriteToLog(exw.Message, "RevisaCarpetas", Application.StartupPath + "\\" + LogFile.Trim()); 
                }
            }//del ciclo de cada lector
        }

        public string ForzaMatchEnBpro(string id_agencia, string id_persona, string clave_sicop)
        {
            string res = "";
            string Q = "Select ip, usr_bd, pass_bd, nombre_bd, centralizada from SICOP_TRASMISION where id_agencia=" + id_agencia.Trim();
            //this.agenciaCentralizada = this.objDB.ConsultaUnSoloCampo("Select centralizada from SICOP_TRASMISION where id_agencia=" + this.id_agencia.Trim()).Trim();  
            
            DataSet ds = this.objDB.Consulta(Q);
            if (!this.objDB.EstaVacio(ds))
            {
                string strconeccion = "Data Source={0}; Initial Catalog={1}; Persist Security Info=True; User ID={2};Password={3}";
                
                foreach (DataRow reg in ds.Tables[0].Rows)
                {
                    strconeccion = string.Format(strconeccion, reg["ip"].ToString().Trim(), reg["nombre_bd"].ToString().Trim(), reg["usr_bd"].ToString().Trim(), reg["pass_bd"].ToString().Trim());
                    this.ConnStringPerPersonasCentralizada = string.Format(this.ConnStringPerPersonasCentralizada, reg["ip"].ToString(), reg["usr_bd"].ToString(), reg["pass_bd"].ToString());   //Data Source={0};Initial Catalog=GA_Corporativa; Persist Security Info=True; User ID={1};Password={2}
                    if (reg["centralizada"].ToString().ToUpper() == "TRUE")
                        strconeccion = this.ConnStringPerPersonasCentralizada.Trim();
                    
                    ConexionBD objConBPro = new ConexionBD(strconeccion);
                    Q = " Select Count(PER_SICOP) from PER_PERSONAS where per_idpersona=" + id_persona;
                    string strcuantos = objConBPro.ConsultaUnSoloCampo(Q).Trim();
                    if (strcuantos.Trim() != "0")
                    {
                        //Averiguamos si alguna otra persona ya tiene este Per_SICOP
                        Q = " Select per_idpersona from PER_PERSONAS where PER_SICOP='" + clave_sicop.Trim() + "' and per_idpersona <> " + id_persona.Trim();
                        string algunaotrapersona = objConBPro.ConsultaUnSoloCampo(Q).Trim();
                        if (algunaotrapersona.Trim() != "")
                        {
                            Utilerias.WriteToLog("Ya existe una persona: " + algunaotrapersona + " Con este IdSicop: " + clave_sicop.Trim() + " se lo quitaremos.", "ForzaMatchEnBpro", Application.StartupPath + "\\" + LogFile.Trim());
                            BorraIdSicop(strconeccion, clave_sicop.Trim(), id_agencia.Trim());  
                        }


                        Q = " Select PER_SICOP from PER_PERSONAS where per_idpersona=" + id_persona;
                        string SICOP_ACTUAL = objConBPro.ConsultaUnSoloCampo(Q).Trim();
                        Utilerias.WriteToLog(" La persona en el campo Telefono2_Trabajo : " + id_persona + " Tiene este id_sicop: " + SICOP_ACTUAL + " ANTES de la actualización ", "ForzaMatchEnBpro", Application.StartupPath + "\\" +LogFile.Trim());                        
                        Q = " Update PER_PERSONAS set PER_SICOP='" + clave_sicop + "' where per_idpersona=" + id_persona.Trim();
                        objConBPro.EjecUnaInstruccion(Q);
                        Q = " Select PER_SICOP from PER_PERSONAS where per_idpersona=" + id_persona;
                        string SICOP_POSTERIOR = objConBPro.ConsultaUnSoloCampo(Q).Trim();
                        Utilerias.WriteToLog(" La persona en el campo Telefono2_Trabajo : " + id_persona + " Tiene este id_sicop: " + SICOP_POSTERIOR + " DESPUES de la actualización ", "ForzaMatchEnBpro", Application.StartupPath + "\\" +LogFile.Trim());
                        res = "Sicop " + clave_sicop + " colocado a : " + id_persona.Trim();   
                    }
                    else {
                        Utilerias.WriteToLog(" No se encontró a la persona en fax : " + id_persona, "ForzaMatchEnBpro", Application.StartupPath + "\\" + LogFile.Trim());
                        res = "La persona " + id_persona.Trim() + " no existe en BPro "; 
                    }
                }
            }

            return res;
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
                Utilerias.WriteToLog(ex.Message, "LanzaEjecucion", Application.StartupPath + "\\" +LogFile.Trim());
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

                string SoloNombreExe = RutaEjecutableBPro.Trim(); //20160308 Para que no se cicle y trabe todo.
                SoloNombreExe = SoloNombreExe.Substring(SoloNombreExe.LastIndexOf("\\") + 1);
                SoloNombreExe = SoloNombreExe.Replace(".EXE", "");
                SoloNombreExe = SoloNombreExe.Replace(".exe", "");
                int instanciasejecutandose = CuentaInstancias(SoloNombreExe);

                if (instanciasejecutandose > 0)
                {
                    Utilerias.WriteToLog("Hay " + instanciasejecutandose.ToString() + " instancias ejecutandose de " + SoloNombreExe.Trim() + ", se procesa de cualquier manera.", "ProcesaArchivo", Application.StartupPath + "\\" + LogFile.Trim());
                    System.Threading.Thread.Sleep(10000); //esperamos 10 segundos.
                }
                                 

                //if (instanciasejecutandose == 0 )
                //{
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
                        Utilerias.WriteToLog("Se ejecutó el comando: " + Comando, "ProcesaArchivo", Application.StartupPath + "\\" + LogFile.Trim());
                        System.Threading.Thread.Sleep(Convert.ToInt16(this.SegundosEspera) * 1000);

                        SoloNombreArchivo = SoloNombreArchivo.Replace(".txt", fechahora + ".txt");

                        if (File.Exists(RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim()))
                            File.Delete(RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim());

                        File.Copy(FullPath.Trim(), RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim());
                        //Utilerias.WriteToLog("Se Movio el archivo: " + RutaDirectorioProcesados + "\\" + SoloNombreArchivo.Trim() + " para respaldo.", "ProcesaArchivo", Application.StartupPath + "\\Log.txt");
                        File.Delete(FullPath.Trim());
                    }
                //}
                //else
                //{
                //    Utilerias.WriteToLog("Hay " + instanciasejecutandose.ToString() + " instancias ejecutandose de " + SoloNombreExe.Trim() + ", no se procesa para evitar el ciclado.", "ProcesaArchivo", Application.StartupPath + "\\Log.txt");
                //    System.Threading.Thread.Sleep(10000); //esperamos 10 segundos.
                //}
            }
            catch (Exception ex)
            {
                Utilerias.WriteToLog(ex.Message, "ProcesaArchivo", Application.StartupPath + "\\" + LogFile.Trim());
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
            ConsultaProspectos();
            RevisaAbreCandado();
        }
        /// <summary>
        /// Revisa la bitacora de abrir|cerrar candado, se conecta a la BD correspondiente y le cambia el estatus al parametro en BPRo.
        /// </summary>
        public void RevisaAbreCandado()
        {
            string Q = "";
            SqlConnection conBP = new SqlConnection();
            SqlCommand bp_comand = new SqlCommand();

            Q = "Select * from SICOP_BITACORA_ABRIRCANDADO where estatus='x abrir'";
            DataSet ds = this.objDB.Consulta(Q);
            if (!this.objDB.EstaVacio(ds))
            {
                foreach (DataRow reg in ds.Tables[0].Rows)
                { 
                  //Abrimos el candado y cambiamos el estatus.
                    #region Consulta de los datos para el Logueo en el Servidor Remoto
                    //conociendo el id_agencia procedemos a consultar los datos de conexion en la tabla transferencia
                    string Q1 = "Select ip,usr_bd,pass_bd,nombre_bd,bd_alterna, dir_remoto_xml, dir_remoto_pdf,usr_remoto,pass_remoto, ip_almacen_archivos, smtpserverhost, smtpport, usrcredential, usrpassword ";
                    Q1 += " From SICOP_TRASMISION where id_agencia='" + reg["id_agencia"].ToString().Trim() + "'";

                    DataSet ds2 = this.objDB.Consulta(Q1);
                    if (!this.objDB.EstaVacio(ds2))
                    {
                        DataRow regConexion = ds2.Tables[0].Rows[0];
                        string strconexionABussinesPro = string.Format("Data Source={0};Initial Catalog={1}; Persist Security Info=True; User ID={2};Password={3}", regConexion["ip"].ToString(), regConexion["bd_alterna"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());

                        if (conBP.State.ToString().ToUpper().Trim() == "CLOSED")
                        {
                            try
                            {
                                conBP.ConnectionString = strconexionABussinesPro;
                                conBP.Open();
                            }
                            catch (Exception exop)
                            {
                                Utilerias.WriteToLog("Error al abrir la base de BP" + exop.Message, "Busca", Application.StartupPath + "\\" + LogFile.Trim());
                            }

                            try
                            {
                                bp_comand.Connection = conBP;
                                //Para abrir el candado se pone en estatus inactivo el parámetro
                                Q = "UPDATE PNC_PARAMETR set PAR_STATUS='I' where PAR_TIPOPARA='IDSICOP' AND PAR_IDENPARA=1 AND PAR_IDMODULO='PER'";
                                bp_comand.CommandText = Q.Trim();
                                int regafect = bp_comand.ExecuteNonQuery();
                                if (regafect == 1)
                                {
                                    this.ntiBalloon.ShowBalloonTip(1, "SICOP REGISTRO DE PROSPECTOS", " Se abre candado x 10 min. para agencia: " + reg["id_agencia"].ToString().Trim(), ToolTipIcon.Info);
                                    Q = "Update SICOP_BITACORA_ABRIRCANDADO set estatus='abierto', fecha=getdate() where id_bitacora=" + reg["id_bitacora"].ToString().Trim() + " and id_agencia='" + reg["id_agencia"].ToString().Trim() + "'";
                                    if (this.objDB.EjecUnaInstruccion(Q) == 1)
                                    { //abrimos un hilo para cerrar el candado en 10 minutos.
                                        hilogenericolocal = new Thread(() => CierraCandadoEn10minutos(strconexionABussinesPro,reg["id_agencia"].ToString().Trim(),reg["id_bitacora"].ToString().Trim()));
                                        hilogenericolocal.IsBackground = true;
                                        hilogenericolocal.Start();                                    
                                    }
                                }
                            }
                            catch (Exception ex1)
                            {
                                Utilerias.WriteToLog("Error: Imposible conexion con BD de BP:" + ex1.Message, "Busca", Application.StartupPath + "\\" +LogFile.Trim());
                            }
                        }
                    }
                    else
                    {
                        Utilerias.WriteToLog("Error: No fue posible autenticarse en el servidor remoto", "Busca", Application.StartupPath + "\\" + LogFile.Trim());
                    }
                    #endregion                  
                }
            }

            #region Revisamos x si se quedo abierto por mas de 20 minutos un candado que debe cerrarse por esta funcionalidad
            Q = "Select * from [dbo].SICOP_BITACORA_ABRIRCANDADO where estatus='abierto' and DateDiff(mi,fecha,GETDATE())>=20";
            DataSet ds3 = this.objDB.Consulta(Q);
            if (!this.objDB.EstaVacio(ds3))
            {
                foreach (DataRow reg1 in ds3.Tables[0].Rows)
                {
                    string Q1 = "Select ip,usr_bd,pass_bd,nombre_bd,bd_alterna, dir_remoto_xml, dir_remoto_pdf,usr_remoto,pass_remoto, ip_almacen_archivos, smtpserverhost, smtpport, usrcredential, usrpassword ";
                    Q1 += " From SICOP_TRASMISION where id_agencia='" + reg1["id_agencia"].ToString().Trim() + "'";

                    DataSet ds2 = this.objDB.Consulta(Q1);
                    if (!this.objDB.EstaVacio(ds2))
                    {
                        DataRow regConexion = ds2.Tables[0].Rows[0];
                        string strconexionABussinesPro = string.Format("Data Source={0};Initial Catalog={1}; Persist Security Info=True; User ID={2};Password={3}", regConexion["ip"].ToString(), regConexion["bd_alterna"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());

                        if (conBP.State.ToString().ToUpper().Trim() == "CLOSED")
                        {
                            try
                            {
                                conBP.ConnectionString = strconexionABussinesPro;
                                conBP.Open();
                            }
                            catch (Exception exop)
                            {
                                Utilerias.WriteToLog("Error al abrir la base de BP" + exop.Message, "Busca", Application.StartupPath + "\\" + LogFile.Trim());
                            }

                            try
                            {
                                bp_comand.Connection = conBP;
                                //Para cerrar el candado se pone en estatus Activo el parámetro
                                Q = "UPDATE PNC_PARAMETR set PAR_STATUS='A' where PAR_TIPOPARA='IDSICOP' AND PAR_IDENPARA=1 AND PAR_IDMODULO='PER'";
                                bp_comand.CommandText = Q.Trim();
                                int regafect = bp_comand.ExecuteNonQuery();
                                if (regafect == 1)
                                {
                                    this.ntiBalloon.ShowBalloonTip(1, "SICOP REGISTRO DE PROSPECTOS", " Se CIERRA candado para agencia: " + reg1["id_agencia"].ToString().Trim(), ToolTipIcon.Info);
                                    Q = "Update SICOP_BITACORA_ABRIRCANDADO set estatus='cerrado', fecha=getdate() where id_bitacora=" + reg1["id_bitacora"].ToString().Trim() + " and id_agencia='" + reg1["id_agencia"].ToString().Trim() + "'";
                                    this.objDB.EjecUnaInstruccion(Q);
                                }
                            }
                            catch (Exception ex1)
                            {
                                Utilerias.WriteToLog("Error: Imposible conexion con BD de BP:" + ex1.Message, "RevisaAbreCandado", Application.StartupPath + "\\" + LogFile.Trim());
                            }
                        }
                    }
                    else
                    {
                        Utilerias.WriteToLog("Error: No fue posible autenticarse en el servidor remoto", "RevisaAbreCandado", Application.StartupPath + "\\" + LogFile.Trim());
                    }

                }
            }
            #endregion

        }

        private void CierraCandadoEn10minutos(string strconexionABussinesPro, string id_agencia, string id_bitacora)
        {
            Thread.Sleep(600000); //
            try
            {
                SqlConnection conBP = new SqlConnection();
                SqlCommand bp_comand = new SqlCommand();

                if (conBP.State.ToString().ToUpper().Trim() == "CLOSED")
                {
                    try
                    {
                        conBP.ConnectionString = strconexionABussinesPro;
                        conBP.Open();
                    }
                    catch (Exception exop)
                    {
                        Utilerias.WriteToLog("Error al abrir la base de BP" + exop.Message, "Busca", Application.StartupPath + "\\" +LogFile.Trim());
                    }

                    try
                    {
                        bp_comand.Connection = conBP;
                        //Para cerrar el candado se pone en estatus ACTIVO el parámetro
                        string Q = "UPDATE PNC_PARAMETR set PAR_STATUS='A' where PAR_TIPOPARA='IDSICOP' AND PAR_IDENPARA=1 AND PAR_IDMODULO='PER'";
                        bp_comand.CommandText = Q.Trim();
                        int regafect = bp_comand.ExecuteNonQuery();
                        if (regafect == 1)
                        {
                            this.ntiBalloon.ShowBalloonTip(1, "SICOP REGISTRO DE PROSPECTOS", " Se CIERRA *candado para agencia: " + id_agencia.Trim(), ToolTipIcon.Info);
                            Q = "Update SICOP_BITACORA_ABRIRCANDADO set estatus='cerrado', fecha=getdate() where id_bitacora=" + id_bitacora.Trim() + " and id_agencia='" + id_agencia.Trim() + "'";
                            this.objDB.EjecUnaInstruccion(Q);
                            //matamos el hilo                                        
                            try
                            {
                                if (hilogenericolocal != null)
                                {
                                    hilogenericolocal.Interrupt();
                                    hilogenericolocal = null;
                                }
                                // if (!this.dicHilos[Folio_Operacion].IsAlive)                           
                            }
                            catch (Exception ex)
                            {
                                Debug.WriteLine(ex.Message);
                                Utilerias.WriteToLog(ex.Message, "CierraCandadoEn10minutos", Application.StartupPath + "\\" + LogFile.Trim());
                            }
                        }
                    }
                    catch (Exception ex1)
                    {
                        Utilerias.WriteToLog("Error: Imposible conexion con BD de BP:" + ex1.Message, "CierraCandadoEn10minutos", Application.StartupPath + "\\" + LogFile.Trim());
                    }
                }
            }
            catch (Exception exgral)
            {
                Utilerias.WriteToLog("Error: " + exgral.Message, "CierraCandadoEn10minutos", Application.StartupPath + "\\" + LogFile.Trim());            
            }
            finally {
                if (hilogenericolocal != null)
                {
                    if (!hilogenericolocal.IsAlive)
                    {
                        hilogenericolocal = null;
                    }
                }
            }
        }


        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            this.ntiBalloon.Visible = false;
            this.ntiBalloon = null;
        }
        
        /// <summary>
        /// Busca en la tabla PER_PERSONAS de BPRO el IDSICOP
        /// </summary>
        /// <param name="PerSICOPenArchivo">ID SICOP a buscar</param>
        /// <param name="NombreEnArchivo">Nombre proporcionado en el archivo de SICOP</param>
        /// <param name="id_agencia">agencia para conocer la bd en la cual buscar</param>
        /// <returns>Vacio si no encontró un prospecto con ese id_sicop</returns>
        public string EscribeLogProspecto(string PerSICOPenArchivo, string NombreEnArchivo,string RFCenArchivo, string id_agencia,string fecha_archivo,string tipo)
        {
            string res = "";
            string Q = "";  
            string campo="PER_SICOP";                    
            string Descripcion="";

                    //conociendo el id_agencia procedemos a consultar los datos de conexion en la tabla transferencia
                    Q = "Select ip,usr_bd,pass_bd,nombre_bd,bd_alterna, dir_remoto_xml, dir_remoto_pdf,usr_remoto,pass_remoto, ip_almacen_archivos, smtpserverhost, smtpport, usrcredential, usrpassword, centralizada ";
                    Q += " From SICOP_TRASMISION where id_agencia='" + id_agencia + "'";

                    DataSet ds = this.objDB.Consulta(Q);
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                    {
                        DataRow regConexion = ds.Tables[0].Rows[0];
                        string strconexionABussinesPro = string.Format("Data Source={0};Initial Catalog={1}; Persist Security Info=True; User ID={2};Password={3}", regConexion["ip"].ToString(), regConexion["bd_alterna"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());
                        this.ConnStringPerPersonasCentralizada = string.Format(this.ConnStringPerPersonasCentralizada, regConexion["ip"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());   //Data Source={0};Initial Catalog=GA_Corporativa; Persist Security Info=True; User ID={1};Password={2}
                        if (regConexion["centralizada"].ToString().ToUpper() == "TRUE")
                            strconexionABussinesPro = this.ConnStringPerPersonasCentralizada.Trim();

                        ConexionBD objDBBP = new ConexionBD(strconexionABussinesPro);
                        if (regConexion["centralizada"].ToString().ToUpper() == "TRUE")
                        {
                            Q = " select  top 1 PER_NOMRAZON,PER_PATERNO,PER_MATERNO,PER_RFC, PER_SICOP, PER_IDPERSONA ";
                            Q += " from PER_PERSONAS,  GA_Corporativa.dbo.cat_idsicop ";
                            Q += " where PER_IDPERSONA = scp_idbpro";
                            Q += " and scp_idsicop = '" + PerSICOPenArchivo.Trim() + "'";
                        }
                        else {
                            Q = "Select top 1 PER_NOMRAZON,PER_PATERNO,PER_MATERNO,PER_RFC, PER_SICOP, PER_IDPERSONA  from PER_PERSONAS";
                            Q += " where " + campo + " = '" + PerSICOPenArchivo + "'";
                        }

                        DataSet dsbp = objDBBP.Consulta(Q);
                        if (!objDBBP.EstaVacio(dsbp))
                        {
                            Descripcion = "El IDSICOP: " + dsbp.Tables[0].Rows[0]["PER_SICOP"].ToString() + " está asociado a la persona: " + dsbp.Tables[0].Rows[0]["PER_IDPERSONA"].ToString() + " - " + dsbp.Tables[0].Rows[0]["PER_NOMRAZON"].ToString().Trim() + " " + dsbp.Tables[0].Rows[0]["PER_PATERNO"].ToString().Trim() + " " + dsbp.Tables[0].Rows[0]["PER_MATERNO"].ToString().Trim() + " - " + dsbp.Tables[0].Rows[0]["PER_RFC"].ToString().Trim();

                            Q = "Insert into SICOP_LOGPROSPECTOS (fecha,id_sicop,nombre,rfc,descripcion,id_agencia,fecha_archivo,tipo)";
                            Q += " values (getdate(),'{0}','{1}','{2}','{3}','{4}','{5}','{6}')";
                            Q = string.Format(Q, PerSICOPenArchivo, NombreEnArchivo, RFCenArchivo, Descripcion, id_agencia, fecha_archivo,tipo);
                            if (this.objDB.EjecUnaInstruccion(Q) > 0)
                            {
                                res = "Registro en bitacora";
                                if (this.Modo.ToUpper().Trim() == "DEBUG")  
                                {
                                    Utilerias.WriteToLog("Se inserta en log: " + Q, "EscribeLogProspecto", Application.StartupPath + "\\" + LogFile.Trim());   
                                }
                            }
                        } //de que no esta vacio el dataset                             
                    }//de que obtuvo el registro de conexion                                                     
            return res;
        }


public string ObtenDeArchivo(string RutaArchivo,string Que)
        {
            string res = "";                          
            string instruccion = "";
            FileStream fs = null;
            StreamReader sr = null;
            string cuote = "\"";

            Que = cuote + Que.Trim() + cuote;

            try
            {
                fs = new FileStream(RutaArchivo, FileMode.Open, FileAccess.ReadWrite);
                sr = new StreamReader(fs,Encoding.Default);

                //Encoding targetEncoding = Encoding.GetEncoding("windows-1252");            
                int cont = 0;
                //int posicion = Que=="\"No_Vin\""?15:18;
                int posicion = 0;
                if (Que == "\"C_Clave\"")
                    posicion = 0;
                if (Que == "\"Apellido_Paterno\"")
                    posicion = 1;
                if (Que == "\"Apellido_Materno\"")
                    posicion = 2;
                if (Que == "\"Nombre_Pila\"")
                    posicion = 3;
                if (Que == "\"NombreFac\"")
                    posicion = 28;
                if (Que == "\"RFC\"")
                    posicion = 27;
                if (Que == "\"Fax\"")
                    posicion = 12;
                if (Que == "\"Telefono2_Trabajo\"")
                    posicion = 20;


                StringBuilder sb = new StringBuilder();
                
                while (!sr.EndOfStream)
                {
                    instruccion = sr.ReadLine();
                    
                    //byte[] encodedBytes = targetEncoding.GetBytes(instruccion);
                    //instruccion = Encoding.Default.GetString(encodedBytes);                    
                    //Console.WriteLine("Encoded bytes: " + BitConverter.ToString(encodedBytes));                    
                    string[] Arr = instruccion.Split(',');
                    if (cont == 0)
                    {//el primer registro trae el nombre de las columnas buscamos en cual está el No_Vin.
                        if (Arr[posicion].Trim() != Que.Trim())
                        {
                            posicion = 0;
                            bool encontrado = false;
                            while (posicion < Arr.Length && !encontrado)
                            {
                                if (Arr[posicion].Trim() == Que.Trim())
                                {
                                    posicion--;
                                    encontrado = true;
                                }
                                posicion++;
                            }
                        }
                    }
                    else
                    {
                        res = Arr[posicion].Trim();
                        res = res.Replace("\"", ""); 
                    }
                    cont++;
                }
            }//del try
            catch (Exception ex)
            {
                Utilerias.WriteToLog("Error al buscar en el archivo creado: " + ex.Message, "ObtenVinDeArchivo", Application.StartupPath + "\\" + LogFile.Trim());
            }
            finally
            {
               if (sr != null)
                sr.Close();
               if (fs != null) 
                 fs.Close();
            }

            return res;
        }


        public void ConsultaProspectos()
        {
            List<string> list = new List<string>(this.dicProspectoNombre.Keys);
            // Loop through list.
            foreach (string PerSICOPenArchivo in list)
            {
                //Console.WriteLine("{0}, {1}", k, d[k]);
                string valor = this.dicProspectoNombre[PerSICOPenArchivo]; //NombreEnArchivo + "|" + id_agencia + "|0|fecha_archivo|RFCenArchivo|tipo"
                string[] valores = valor.Split('|');
                string NombreEnArchivo = valores[0];
                string id_agencia = valores[1];
                string contador = valores[2]; //son las veces (minutos) que se espera para el registro y posterior envio del mensaje
                string fecha_archivo = valores[3];
                string RFCenArchivo = valores[4];
                string tipo = valores[5];

                string res = EscribeLogProspecto(PerSICOPenArchivo, NombreEnArchivo, RFCenArchivo, id_agencia, fecha_archivo,tipo);
                if (res.Trim() != "")
                {
                    this.dicProspectoNombre.Remove(PerSICOPenArchivo);
                }
                else
                {
                    int contaux = Convert.ToInt32(contador) + 1;
                    if (contaux <= 6)
                        this.dicProspectoNombre[PerSICOPenArchivo] = NombreEnArchivo + "|" + id_agencia + "|" + contaux.ToString() + "|" + fecha_archivo.Trim() + "|" + RFCenArchivo.Trim() + "|" + tipo.Trim();
                    else
                    {
                        string Descripcion = "El prospecto: " + PerSICOPenArchivo.Trim() +  " no está registrado en BPro";
                        string Q = "Insert into SICOP_LOGPROSPECTOS (fecha,id_sicop,nombre,rfc,descripcion,id_agencia,fecha_archivo,tipo)";
                        Q += " values (getdate(),'{0}','{1}','{2}','{3}','{4}','{5}','{6}')";
                        Q = string.Format(Q, PerSICOPenArchivo, NombreEnArchivo, RFCenArchivo, Descripcion, id_agencia, fecha_archivo,tipo);
                        this.objDB.EjecUnaInstruccion(Q);
                        this.dicProspectoNombre.Remove(PerSICOPenArchivo);
                    }
                }
            }             
        }

        public string BorraIdSicop(string strconexionABussinesPro, string id_sicopquitar,string id_agencia)
        {
            string res = "";
            #region Consulta de los datos para el Logueo en el Servidor Remoto
            SqlConnection conBP = new SqlConnection();
            SqlCommand bp_comand = new SqlCommand();

            //conociendo el id_agencia procedemos a consultar los datos de conexion en la tabla transferencia
            //string Q1 = "Select ip,usr_bd,pass_bd,nombre_bd,bd_alterna, dir_remoto_xml, dir_remoto_pdf,usr_remoto,pass_remoto, ip_almacen_archivos, smtpserverhost, smtpport, usrcredential, usrpassword ";
            //Q1 += " From SICOP_TRASMISION where id_agencia='" + this.id_agencia.Trim() + "'";

            //DataSet ds = this.objDB.Consulta(Q1);
            //if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
            //{
                //DataRow regConexion = ds.Tables[0].Rows[0];
                //string strconexionABussinesPro = string.Format("Data Source={0};Initial Catalog={1}; Persist Security Info=True; User ID={2};Password={3}", regConexion["ip"].ToString(), regConexion["bd_alterna"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());

                if (conBP.State.ToString().ToUpper().Trim() == "CLOSED")
                {
                    try
                    {
                        conBP.ConnectionString = strconexionABussinesPro;
                        conBP.Open();
                    }
                    catch (Exception exop)
                    {
                        Utilerias.WriteToLog("Error al abrir la base de BP" + exop.Message, "Busca", Application.StartupPath + "\\" + LogFile.Trim());
                    }

                    try
                    {
                        bp_comand.Connection = conBP;
                        #region String con codigo del store procedure, no se hace un sp porque la bd no es mia.
                        string Q = " Declare ";
                        Q += " @iContador int, ";
                        Q += " @iId int, ";
                        Q += " @sReferencia varchar(15) ";
                        Q += " set nocount on ";
                        Q += " Select @iId = 0 ";
                        Q += " if exists (select 1 from dbo.sysobjects where id = object_id(N'[dbo].[#Ids]')) ";
                        Q += " begin ";
                        Q += "   DROP table #Ids ";
                        Q += " end ";
                        Q += " CREATE TABLE #Ids ";
                        Q += " ( ";
                        Q += "    id_local [int] IDENTITY (1, 1), ";
                        Q += "    IDPERSONA int ";
                        Q += " ) ";
                        Q += " insert into #Ids (IDPERSONA) ";
                        Q += " select distinct s.PER_IDPERSONA ";
                        Q += " from PER_PERSONAS s ";
                        Q += " where s.PER_SICOP='{0}' ";
                        Q += " order by 1 ";
                        Q += " Select @iContador=0	";
                        Q += " Select @sReferencia='' ";

                        Q += " WHILE EXISTS   (Select 1 from #Ids) ";
                        Q += "  BEGIN ";
                        Q += "   SET ROWCOUNT 1 ";
                        Q += "  	Select @iId=IDPERSONA FROM #Ids ";
                        Q += " SET ROWCOUNT 0  ";
                        Q += " if ( @iId<>0 ) ";
                        Q += " begin		";
                        //Q += " print '@sReferencia= ' + @sReferencia + ' @iId= ' + ltrim(rtrim(Convert(char(5),@iId)))	";
                        Q += " update PER_PERSONAS set PER_SICOP ='' ";
                        Q += " where PER_IDPERSONA = @iId ";
                        Q += " Select @iContador = @iContador + 1 ";
                        Q += " end ";
                        Q += " Delete #Ids where IDPERSONA = @iId ";
                        Q += " Select @iId=0		";
                        Q += " Select @sReferencia='' ";
                        Q += "   END	";
                        Q += " Select @iContador ";
                        Q += " set nocount off ";
                        #endregion

                        bp_comand.CommandText = string.Format(Q, id_sicopquitar.Trim());
                        object regafect = bp_comand.ExecuteScalar();
                        if (Convert.ToInt32(regafect) > 0)
                        {
                            Q = "Insert into SICOP_BITACORAUPDATEID (fecha,quien,que,aquien,id_agencia)";
                            Q += " values (getdate(),'ActualizaProspectos|BorraIdSicop|ForzaMatch','Actualizacion directa del ID SICOP: " + id_sicopquitar.Trim() + "','" + id_sicopquitar.Trim() + "','" + id_agencia.Trim() + "')";
                            this.objDB.EjecUnaInstruccion(Q);
                            Utilerias.WriteToLog("Se quitó el id_sicop: " + id_sicopquitar + " de la base: " + strconexionABussinesPro, "BorraIdSicop", Application.StartupPath + "\\" + LogFile.Trim());
                            res = "Se quitó ID_SICOP ";
                        }
                    }
                    catch (Exception ex1)
                    {
                        MessageBox.Show(ex1.Message);
                        Utilerias.WriteToLog("Error: Imposible conexion con BD de BP:" + ex1.Message, "Busca", Application.StartupPath + "\\" + LogFile.Trim());
                        res = "Error no se quitó el id_sicop";
                    }
                }
            //}
            //else
            //{
              //  Utilerias.WriteToLog("Error: No fue posible autenticarse en el servidor remoto", "Busca", Application.StartupPath + "\\Log.txt");
            //}
            #endregion
            return res;

        }

        #region Respaldo comentariado
        /*
        /// <summary>
        /// Busca en la tabla PER_PERSONAS de BPRO el IDSICOP
        /// </summary>
        /// <param name="PerSICOPenArchivo">ID SICOP a buscar</param>
        /// <param name="NombreEnArchivo">Nombre proporcionado en el archivo de SICOP</param>
        /// <param name="id_agencia">agencia para conocer la bd en la cual buscar</param>
        /// <returns>Vacio si no encontró un prospecto con ese id_sicop</returns>
        public string EscribeLogProspecto(string PerSICOPenArchivo, string NombreEnArchivo, string RFCenArchivo, string id_agencia, string fecha_archivo)
        {
            string res = "";
            string Q = "";
            string campo = "PER_SICOP";
            string Descripcion = "";

            //conociendo el id_agencia procedemos a consultar los datos de conexion en la tabla transferencia
            Q = "Select ip,usr_bd,pass_bd,nombre_bd,bd_alterna, dir_remoto_xml, dir_remoto_pdf,usr_remoto,pass_remoto, ip_almacen_archivos, smtpserverhost, smtpport, usrcredential, usrpassword, centralizada ";
            Q += " From SICOP_TRASMISION where id_agencia='" + id_agencia + "'";

            DataSet ds = this.objDB.Consulta(Q);
            if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
            {
                DataRow regConexion = ds.Tables[0].Rows[0];
                string strconexionABussinesPro = string.Format("Data Source={0};Initial Catalog={1}; Persist Security Info=True; User ID={2};Password={3}", regConexion["ip"].ToString(), regConexion["bd_alterna"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());
                this.ConnStringPerPersonasCentralizada = string.Format(this.ConnStringPerPersonasCentralizada, regConexion["ip"].ToString(), regConexion["usr_bd"].ToString(), regConexion["pass_bd"].ToString());   //Data Source={0};Initial Catalog=GA_Corporativa; Persist Security Info=True; User ID={1};Password={2}
                if (regConexion["centralizada"].ToString().ToUpper() == "TRUE")
                    strconexionABussinesPro = this.ConnStringPerPersonasCentralizada.Trim();

                ConexionBD objDBBP = new ConexionBD(strconexionABussinesPro);
                Q = "Select top 1 PER_NOMRAZON,PER_PATERNO,PER_MATERNO,PER_RFC, PER_SICOP, PER_IDPERSONA  from PER_PERSONAS";
                Q += " where " + campo + " = '" + PerSICOPenArchivo + "'";
                DataSet dsbp = objDBBP.Consulta(Q);
                if (!objDBBP.EstaVacio(dsbp))
                {
                    Descripcion = "El IDSICOP: " + dsbp.Tables[0].Rows[0]["PER_SICOP"].ToString() + " está asociado a la persona: " + dsbp.Tables[0].Rows[0]["PER_IDPERSONA"].ToString() + " - " + dsbp.Tables[0].Rows[0]["PER_NOMRAZON"].ToString().Trim() + " " + dsbp.Tables[0].Rows[0]["PER_PATERNO"].ToString().Trim() + " " + dsbp.Tables[0].Rows[0]["PER_MATERNO"].ToString().Trim() + " - " + dsbp.Tables[0].Rows[0]["PER_RFC"].ToString().Trim();

                    Q = "Insert into SICOP_LOGPROSPECTOS (fecha,id_sicop,nombre,rfc,descripcion,id_agencia,fecha_archivo)";
                    Q += " values (getdate(),'{0}','{1}','{2}','{3}','{4}','{5}')";
                    Q = string.Format(Q, PerSICOPenArchivo, NombreEnArchivo, RFCenArchivo, Descripcion, id_agencia, fecha_archivo);
                    if (this.objDB.EjecUnaInstruccion(Q) > 0)
                    {
                        res = "Registro en bitacora";
                        if (this.Modo.ToUpper().Trim() == "DEBUG")
                        {
                            Utilerias.WriteToLog("Se inserta en log: " + Q, "EscribeLogProspecto", Application.StartupPath + "\\Log.txt");
                        }
                    }
                } //de que no esta vacio el dataset                             
            }//de que obtuvo el registro de conexion                                         

            return res;
        }
        */

        #endregion


    }
}
