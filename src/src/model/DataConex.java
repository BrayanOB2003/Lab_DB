package model;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataConex {

    public DataConex(){
        conex();
    }
    private void conex(){
        // Establecer la conexión a la base de datos Oracle
        Connection connection = null;
        try {
            DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
            connection = DriverManager.getConnection("jdbc:oracle:thin:@200.3.193.24:1522/ESTUD", "P09551_1_8", "ortiz2003");
            Statement stmt = connection.createStatement();
            //Crear tablas
            executeTableQueries(stmt);

            //Inserts
            executeAllInsertQueries(connection);

            connection.close();
            //dropTables(stmt);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private void executeTableQueries(Statement statement) throws SQLException {
        List<String> queries = Queries.getTableQueries();
        ResultSet rst = null;
        for (int i = 0; i < queries.size(); i++) {
            rst = statement.executeQuery(queries.get(i));
        }
        statement.close();
    }

    private void executeAllInsertQueries(Connection connection) throws SQLException, IOException {

        executeInsertQueries(connection, "src/data/u.user", "\\|", "USERS");

        executeInsertQueries(connection, "src/data/u.item", "\\|","ITEM");

        executeInsertQueries(connection, "src/data/u.genre", "\\|","GENRE");

        executeInsertQueries(connection, "src/data/u.data", "\t","DATA");
    }
    private void executeInsertQueries(Connection connection, String path, String split, String tableName) throws SQLException {
        Statement statement = connection.createStatement();
        PreparedStatement preparedStatement = null;
        ResultSet rs = statement.executeQuery( "SELECT * FROM " + tableName); // Consulta con un límite de 0 para obtener solo la estructura de la tabla
        List<String> columnNames = new ArrayList<>();
        List<Integer> columnType = new ArrayList<>();

        int columnCount = rs.getMetaData().getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            columnNames.add(rs.getMetaData().getColumnName(i));
            columnType.add(rs.getMetaData().getColumnType(i));
        }
        statement.close();
        System.out.println(columnType.toString());

        List<String[]> data = DataReader.readFile(path,split);

        String query = "";
        String aux1 = columnNames.toString().replace("[","").replace("]","");;
        String aux2 = "";

        for(int j = 1; j < data.get(0).length; j++) {
            aux2 += "?, ";
        }
        aux2 += "?";

        Timestamp timestamp = new Timestamp(0L);

        for(int i = 0; i < data.size(); i++){

            query = "INSERT INTO " + tableName + "(" + aux1 + ") VALUES(" + aux2 + ")";
            preparedStatement = connection.prepareStatement(query);
            for(int j = 0; j < columnType.size();j++){

                if(columnType.get(j) == 2){
                    preparedStatement.setInt(j+1, Integer.parseInt(data.get(i)[j]));

                } else if(columnType.get(j) == 12){
                    preparedStatement.setString(j+1, data.get(i)[j]);
                } else if(columnType.get(j) == 93) {
                    timestamp.setTime(Long.parseLong(data.get(i)[j]) *1000L);
                    preparedStatement.setTimestamp(j+1, timestamp);
                }
            }

            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
    }
    private void dropTables(Statement statement) throws SQLException {
        statement.executeQuery(Queries.dropTables);
    }
}
