using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace ConsoleApplication1
{
    class Program
    {
        MySqlConnection conn;
        String strConn;

        public Program()
        {
            //En el constructor encontramos el string necesario para conectar con la base de datos
            strConn = "server = 127.0.0.1;" +
                            "uid = root;" +
                            "pwd =;" +
                            "database = db_muestras_01;";
        }

        static void Main(string[] args)
        {
            for (int j = 1; j <= 1; j++)
            {

                Program pr = new Program();

                Console.WriteLine("Ingresa el numero de estación que deseas procesar");
                string estacion = j.ToString();
                pr.conectar();

                string mayor = pr.mayor(estacion);
                int maximo = pr.maximo(estacion);
                string fecha;
                //En esta lista almacenaremos los valores de las muestras
                List<string> valores = new List<string>();
                for (int i = 0; i < maximo; i++)
                {
                    valores.Clear();
                    fecha = pr.fecha(estacion, mayor, i);
                    valores = pr.calcular(estacion, fecha);
                    pr.generaRegistro(estacion, fecha, valores);

                }

                pr.cerrar();
                Console.WriteLine("============FINALIZADO, ESTACIÓN: " + j.ToString() +  "============");
            }
        }

        public void generaRegistro(string estacion, string fecha, List<string> valores)
        {
            string registro = null;
            registro += estacion + ";";
            registro += fecha + ";";
            foreach (string valor in valores)
            {
                registro += valor + ";";
            }
            if (valores.Count == 7)
            {
                registro += asimilacion(valores);
            }
            try
            {
                string fileName = "Estacion_" + estacion + ".csv";
                // esto inserta texto en un archivo existente, si el archivo no existe lo crea
                StreamWriter writer = File.AppendText(fileName);
                writer.WriteLine(registro);
                writer.Close();
            }
            catch
            {
                Console.WriteLine("Error");
            }
        }

        private string asimilacion(List<string> valores)
        {
            double cond = 310.5;
            double od = 4.41;
            double ph = 6.31;
            double orp = 43.4;
            double sac = 0.6;
            double temp = 21.1;
	        double turb = 1;

            double aux1 = Math.Abs((cond - double.Parse(valores[0]))/cond);
            double aux2 = Math.Abs((od - double.Parse(valores[1])) / od);
            double aux3 = Math.Abs((ph - double.Parse(valores[2])) / ph);
            double aux4 = Math.Abs((orp - double.Parse(valores[3])) / orp);
            double aux5 = Math.Abs((sac - double.Parse(valores[4])) / sac);
            double aux6 = Math.Abs((temp - double.Parse(valores[5])) / temp);
            double aux7 = Math.Abs((turb - double.Parse(valores[6])) / turb);

            double asimilacion = aux1 + aux2 + aux3 + aux4 + aux5 + aux6;// +aux7;
            return asimilacion.ToString();
        }

        //Método que proporciona la conexión del programa con la base de datos
        private void conectar(){
            try{
                conn = new MySqlConnection();
                conn.ConnectionString = strConn;
                conn.Open();
                Console.WriteLine("La conexión se ha realizado con éxito.");
            }
            catch(MySqlException ex){
                Console.WriteLine(ex.Message);
            }
        }

        private string mayor(string estacion)
        {
            string mayor;
            MySqlCommand instr = conn.CreateCommand();
            //Esta consulta devuelve la muestra con el numero de registros más grande de la base de datos
            instr.CommandText = "SELECT `id_muestra`, COUNT(*) FROM `tbl_rel_est_mue` WHERE `id_estacion` = " + estacion + " GROUP BY `id_muestra` ORDER BY COUNT(*) DESC LIMIT 0,1;";
            MySqlDataReader reader = instr.ExecuteReader();
            reader.Read();
            mayor =  reader["id_muestra"].ToString();
            reader.Dispose();
            return mayor;
        }

        private int maximo(string estacion)
        {
            int maximo;
            MySqlCommand instr = conn.CreateCommand();
            //Esta consulta devuelve la muestra con el numero de registros más grande de la base de datos
            instr.CommandText = "SELECT `id_muestra`, COUNT(*) FROM `tbl_rel_est_mue` WHERE `id_estacion` = " + estacion + " GROUP BY `id_muestra` ORDER BY COUNT(*) DESC LIMIT 0,1;";
            MySqlDataReader reader = instr.ExecuteReader();
            reader.Read();
            maximo = int.Parse(reader["COUNT(*)"].ToString());
            reader.Dispose();
            return maximo;
        }

        private string fecha(string estacion, string mayor, int i)
        {
            string date;
            MySqlCommand instr = conn.CreateCommand();
            //Esta consulta devuelve la fecha del registro n, servira para igualar las fechas de todas las muestras
            instr.CommandText = "SELECT fecha FROM tbl_rel_est_mue WHERE id_estacion = " + estacion + " AND id_muestra = " + mayor + " ORDER BY fecha ASC LIMIT " + i + ",1;";
            MySqlDataReader reader = instr.ExecuteReader();
            reader.Read();
            date = reader["fecha"].ToString();
            reader.Dispose();
            //procedimiento para dar formato valido para MySql a la fecha
            DateTime dateValue = DateTime.Parse(date);
            return dateValue.ToString("yyyy-MM-dd HH:mm:ss");
        }

        private List<string> calcular(string estacion, string fecha)
        {
            List<string> valores = new List<string>();
            int x = 0;
            MySqlCommand instr = conn.CreateCommand();
            //Se buscan los valores que corresponden al intervalo con la finalidad de saber dada una estacion y una hora, cuales fueron los valores de las muestras.
            //Las muestras no siempre se toman en el mismo intante (precision de segundos) por eso se da un intervalo de un par de minutos
            instr.CommandText = "SELECT B.id_muestra, A.valor FROM tbl_muestras AS B LEFT JOIN (SELECT * FROM tbl_rel_est_mue WHERE id_estacion = " + estacion + " AND fecha BETWEEN  '" + fecha + "' AND DATE_ADD('" + fecha + "', interval 14 minute)) AS A ON B.id_muestra = A.id_muestra ORDER BY B.id_muestra ASC;";
            MySqlDataReader reader = instr.ExecuteReader();
            while (reader.Read())
            {
                //valores.Insert(int, string)
                //int.Parse(reader["id_muestra"].ToString()) --> Genera un valor entero para que pueda ser aceptado por el método Insert
                //reader["valor"].ToString() --> Genera un string para que pueda ser aceptado por el método Insert
                valores.Insert(x, reader["valor"].ToString());
                x++;
            }
            reader.Dispose();
            return valores;
        }



        private void cerrar()
        {
            conn.Close();
        }
    }

}
