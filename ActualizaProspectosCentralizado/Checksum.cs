using System;
using System.IO;
using System.Text;
using System.Security.Cryptography;

namespace ActualizaProspectos
{
	/// <summary>
	/// Algoritmos permitidos
	/// </summary>
	public enum Algorithm
	{
		MD5,
		SHA1,
		SHA256,
		SHA384,
		SHA512
	}

	/// <summary>
	/// Permite calcular valores Hash de cadenas y archivos.
	/// </summary>
	public class Checksum 
	{
		/// <summary>
		/// Calcula el valor hash para la cadena pasada como parámetro.
		/// </summary>
		/// <param name="cadena">Cadena a procesar.</param>
		/// <param name="alg">Algoritmo que vamos a utilizar.</param>
		/// <returns>Cadena representando el valor Hash Obtenido.</returns>
		public string CalculateStringHash( string clearstring, Algorithm alg )
		{
			// del algoritmo seleccionado obtenemos un proveedor de cifrado. 
			HashAlgorithm hash = GetHashProvider( alg );
			// obtenemos un array de bytes de los caracteres dentro de la cadena.
			byte[] tempSource = Encoding.ASCII.GetBytes( clearstring );
			
			// el método ComputeHash calcula el valor Hash y devuelve un array de bytes.
			// convertimos el array a string antes de devolverlo.
			return ArrayToString( hash.ComputeHash(tempSource) );
		}
		
		/// <summary>
		/// Calcula el valor Hash del archivo pasado como parámetro.
		/// </summary>
		/// <param name="filename">Nombre completo del archivo a procesar.</param>
		/// <param name="alg">Algortimo que vamos a utilizar.</param>
		/// <returns>Cadena representando el valor Hash del archivo.</returns>
		public string CalculateFileHash( string filename, Algorithm alg )
		{
			// del algoritmo seleccionado obtenemos un proveedor de cifrado.
			HashAlgorithm hash = GetHashProvider( alg );
			// creamos un objeto stream con el archivo especificado.
			FileStream fs = new FileStream( filename, FileMode.Open, FileAccess.Read );

			// el método ComputeHash calcula el valor Hash del flujo 
			// que representa al archivo. convertimos el array a string y lo asignamos
			// a una variable
			string resul = ArrayToString( hash.ComputeHash(fs) );
			fs.Close(); // importante!! cerramos el flujo.

			return resul; // devolvemos el valor de la variable de cadena.
		}
		
		/// <summary>
		/// Convierte un Array de bytes en una cadena de caracteres.
		/// </summary>
		/// <param name="byteArray">Byte array origen.</param>
		/// <returns>Cadena de caracteres obtenida del array.</returns>
		private string ArrayToString( byte[] byteArray )
		{
			// usamos un objeto stringbuilder por su mejor rendimiento con operaciones
			// de cadenas.
			StringBuilder sb = new StringBuilder( byteArray.Length );
			
			// por cada byte en el array
			for ( int i = 0; i < byteArray.Length; i++ )
			{
				// obtenemos su valor hexadecimal y lo agregamos al objeto
				// stringbuilder.
				sb.Append( byteArray[i].ToString("X2") );
			}
			
			// devolvemos el objeto stringbuilder, formateado a string
			return sb.ToString();
		}
		
		/// <summary>
		/// Obtiene un proveedor HashAlgorithm de la enumeración utilizada.
		/// </summary>
		/// <param name="alg">Algoritmo seleccionado.</param>
		/// <returns>Proveedor Hash correpondiente al algoritmo deseado.</returns>
		private HashAlgorithm GetHashProvider( Algorithm alg )
		{
			switch ( alg )
			{
				case Algorithm.MD5:
					return new MD5CryptoServiceProvider();
				case Algorithm.SHA1:
					return new SHA1CryptoServiceProvider();
				case Algorithm.SHA256:
					return new SHA256Managed();	
				case Algorithm.SHA384:
					return new SHA384Managed();
				case Algorithm.SHA512:
					return new SHA512Managed();
				default: // sino se ha encontrado un valor correspondiente en la
					// enumeración, generamos un excepción para indicarlo.
					throw new Exception( "Invalid Provider." );
			}
		}
	}
}
