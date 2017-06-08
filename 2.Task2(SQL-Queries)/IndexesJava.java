import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class PrintIndexedResultSet {
  public static void main(String args[]) throws Exception {
    String query = "SELECT STATE, COUNT(STATE) FROM MEMBER_PROFILES GROUP BY STATE";
    Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
    Connection con = DriverManager.getConnection("jdbc:odbc:Members");
    Statement stmt = con.createStatement();
    stmt.executeUpdate("CREATE INDEX STATE_INDEX ON MEMBER_PROFILES(STATE)");

    java.util.Date startTime = new java.util.Date();

    ResultSet rs = stmt.executeQuery(query);
    ResultSetMetaData md = rs.getMetaData();

    int nColumns = md.getColumnCount();
    for (int i = 1; i <= nColumns; i++) {
      System.out.print(md.getColumnLabel(i) + ((i == nColumns) ? "\n" : "\t"));
    }

    while (rs.next()) {
      for (int i = 1; i <= nColumns; i++) {
        System.out.print(rs.getString(i) + ((i == nColumns) ? "\n" : "\t"));
      }
    }
    java.util.Date endTime = new java.util.Date();
    long elapsedTime = endTime.getTime() - startTime.getTime();
    System.out.println("Elapsed time: " + elapsedTime);

    stmt.executeUpdate("DROP INDEX MEMBER_PROFILES.STATE_INDEX");
  }
}