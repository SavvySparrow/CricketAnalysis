import java.sql.*;

/**
 * Created by sahiljalan on 14/4/17.
 */
public class CreateDB {
    private static String DriverName = "org.apache.hive.jdbc.HiveDriver";
    private static Statement query;
    private static Connection conection;

    public static void main(String[] arg) throws SQLException {

        try{
            Class.forName(DriverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conection = DriverManager.getConnection("jdbc:hive2://localhost:10000/HiveMetastore_DB" +
                "","Savvy","Tbijmkbmbo2@");
        query = conection.createStatement();
        String tableName = "Hive_test_table";
        query.execute("drop table if exists " + tableName);
        query.execute("create table " + tableName + " (key int, value string)");


        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = query.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = query.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }


    }
}
