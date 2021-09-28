package Framework.Demo.MSSQL;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    private static final String configAppPath = rootPath + "config.properties";
    private static final Properties configAppProperties = new Properties();
    private static final Logger LOGGER  = Logger.getLogger(Main.class.toString());

    private static final String jdbcDriver = "";
    private static final String jdbcURL = "";

    public static void main(String[] args) throws IOException {
	// write your code here
        LOGGER.log(Level.INFO, "Proceso exitoso");
        LOGGER.info("Ruta de archivo de configuracion: " + configAppPath );

        // carga de informacion de properties
        configAppProperties.load(new FileInputStream(configAppPath));
        LOGGER.info(configAppProperties.toString());

        validaDriver();
        obtenemosDatosServerDB();
        obtenemosDatosResultSet();

        System.out.println("Program finished");
    }

    /**
     * validamos que exista el driver de conexion con mssql
     */
    public static void validaDriver(){
        try
        {
            Class.forName(configAppProperties.getProperty("db.jdbcdriver")).newInstance();
            System.out.println("JDBC driver loaded");
        }
        catch (Exception err)
        {
            System.err.println("Error loading JDBC driver");
            err.printStackTrace(System.err);
            System.exit(0);
        }
    }

    public static void obtenemosDatosServerDB(){
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(
                    configAppProperties.getProperty("db.url"),
                    configAppProperties.getProperty("db.username"),
                    configAppProperties.getProperty("db.password"));
            if (conn != null) {
                DatabaseMetaData dm = (DatabaseMetaData) conn.getMetaData();
                System.out.println("Driver name: " + dm.getDriverName());
                System.out.println("Driver version: " + dm.getDriverVersion());
                System.out.println("Product name: " + dm.getDatabaseProductName());
                System.out.println("Product version: " + dm.getDatabaseProductVersion());
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
    /**
     * Ejecutamos query en baase de datos y obtenemos la colleccion de resultSet
     *
     * */
    public static void obtenemosDatosResultSet(){
        Connection databaseConnection= null;

        try
        {
            //conectamos con la base de datos
            databaseConnection = DriverManager.getConnection(
                    configAppProperties.getProperty("db.url"),
                    configAppProperties.getProperty("db.username"),
                    configAppProperties.getProperty("db.password"));

            System.out.println("Connected to the database");

            //declaramos la variable de ejecucion de base de datos
            Statement sqlStatement = databaseConnection.createStatement();

            //declaramos la variable de regreso de resultados
            ResultSet rs = null;

            //construccion de query SQL
            String queryString = "";
            queryString += "select ";
            queryString += "op.OrganizationName as ParentOrganizationName, ";
            queryString += "oc.OrganizationName as OrganizationName, ";
            queryString += "c.CurrencyName as CurrencyName ";
            queryString += "from dbo.DimOrganization as oc ";
            queryString += "inner join dbo.DimOrganization as op on op.OrganizationKey=oc.ParentOrganizationKey ";
            queryString += "inner join dbo.DimCurrency as c on oc.CurrencyKey=c.CurrencyKey ";
            queryString += "order by ParentOrganizationName, OrganizationName ";

            //imprime el query que se va a ejecutar
            System.out.println("\nQuery string:");
            System.out.println(queryString);

            //ejecuta query
            rs=sqlStatement.executeQuery(queryString);

            //Imprime la cabecera de los registros
            System.out.println("\nParentOrganizationName\t\t|\tOrganizationName\t\t|\tCurrencyName");
            System.out.println("----------------------\t\t|\t----------------\t\t|\t------------");

            //loop through the result set and call method to print the result set row
            while (rs.next())
            {
                printResultSetRow(rs);
            }

            //close the result set
            rs.close();
            System.out.println("Closing database connection");

            //close the database connection
            databaseConnection.close();
        }
        catch (SQLException err)
        {
            System.err.println("Error connecting to the database");
            err.printStackTrace(System.err);
            System.exit(0);
        }
    }

    /**
     * Imprime los datos del renglon del ResutSet
     *
     * @param  rs  the result set from the SELECT query
     * @throws SQLException SQLException thrown on error
     */
    public static void printResultSetRow(ResultSet rs) throws SQLException
    {
        //Use the column name alias as specified in the above query
        String OrganizationName= rs.getString("OrganizationName");
        String ParentOrganizationName= rs.getString("ParentOrganizationName");
        String CurrencyName= rs.getString("CurrencyName");
        System.out.println(ParentOrganizationName+"\t\t|\t"+ OrganizationName + "\t\t|\t" + CurrencyName);
    }

}
