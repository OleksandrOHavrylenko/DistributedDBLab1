package com.distributed.databases.lab1;

import com.distributed.databases.lab1.model.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author Oleksandr Havrylenko
 **/
public class CounterTester {
    private final static Logger logger = LoggerFactory.getLogger(CounterTester.class);
    public static final int USER_ID = 1;

    public static void test1(final int maxCounterVal, boolean isIsolationSerializable) {
        ;
        try (Connection conn = ConnectionUtils.getConnection()) {
            conn.setAutoCommit(false);

            if (isIsolationSerializable) {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            }

            PreparedStatement counterStmt = conn.prepareStatement("SELECT counter FROM user_counter WHERE user_id = ?");
            counterStmt.setInt(1, USER_ID);
            PreparedStatement updateStmt = conn.prepareStatement("UPDATE user_counter SET counter = ? WHERE user_id = ?");

            ResultSet resultSet;
            for (int i = 0; i < maxCounterVal; i++) {
                resultSet = counterStmt.executeQuery();
                resultSet.next();
                int counter = resultSet.getInt("counter");
                counter = counter + 1;
                updateStmt.setInt(1, counter);
                updateStmt.setInt(2, USER_ID);
                updateStmt.executeUpdate();
                conn.commit();
            }
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            logger.error("Update counter failed while Lost-Update test." +
                    " message = {}, state = {}, code = {};", e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }
    }

    public static void test2(final int maxCounterVal) {
        try (Connection conn = ConnectionUtils.getConnection()) {
            conn.setAutoCommit(false);

            PreparedStatement inPlaceUpdate = conn.prepareStatement("UPDATE user_counter SET counter = counter + 1 WHERE user_id = ?");
            inPlaceUpdate.setInt(1, USER_ID);

            for (int i = 0; i < maxCounterVal; i++) {
                inPlaceUpdate.executeUpdate();
                conn.commit();
            }
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            logger.error("Update counter failed while In-Place update test." +
                    " message = {}, state = {}, code = {};", e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }
    }

    public static void test3(final int maxCounterVal) {
        ;
        try (Connection conn = ConnectionUtils.getConnection()) {
            conn.setAutoCommit(false);

            PreparedStatement counterForUpdate = conn.prepareStatement("SELECT counter FROM user_counter WHERE user_id = ? FOR  UPDATE");
            counterForUpdate.setInt(1, USER_ID);
            PreparedStatement updateStmt = conn.prepareStatement("UPDATE user_counter SET counter = ? WHERE user_id = ?");

            ResultSet resultSet;
            for (int i = 0; i < maxCounterVal; i++) {
                resultSet = counterForUpdate.executeQuery();
                resultSet.next();
                int counter = resultSet.getInt("counter");
                counter = counter + 1;
                updateStmt.setInt(1, counter);
                updateStmt.setInt(2, USER_ID);
                updateStmt.executeUpdate();
                conn.commit();
            }
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            logger.error("Update counter failed while Row-level locking test." +
                    " message = {}, state = {}, code = {};", e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }
    }

    public static void test4(final int maxCounterVal) {
        ;
        try (Connection conn = ConnectionUtils.getConnection()) {

            conn.setAutoCommit(false);

            PreparedStatement counterStmt = conn.prepareStatement(
                    "SELECT counter, version FROM user_counter WHERE user_id = ?");
            counterStmt.setInt(1, USER_ID);

            PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE user_counter SET counter = ?, version = ? WHERE user_id = ? and version = ?");

            ResultSet resultSet;
            for (int i = 0; i < maxCounterVal; i++) {
                while (true) {//
                    resultSet = counterStmt.executeQuery();
                    resultSet.next();
                    int counter = resultSet.getInt("counter");
                    int version = resultSet.getInt("version");
                    counter = counter + 1;

                    updateStmt.setInt(1, counter);
                    updateStmt.setInt(2, version + 1);
                    updateStmt.setInt(3, USER_ID);
                    updateStmt.setInt(4, version);

                    int rowCount = updateStmt.executeUpdate();
                    conn.commit();
                    if (rowCount > 0) {
                        break;
                    }
                }
            }
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            logger.error("Update counter failed while Lost-Update test." +
                    " message = {}, state = {}, code = {};", e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }
    }


    public static void createDB() {
        try (Connection conn = ConnectionUtils.getConnection();
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.executeUpdate("DROP TABLE IF EXISTS user_counter;");
            stmt.executeUpdate("CREATE TABLE user_counter(user_id SERIAL, counter serial, version serial)");
            stmt.executeUpdate("INSERT into user_counter (user_id, counter, version) values (1, 0, 0)");
            conn.commit();
            logger.info("Created user_counter table.");
        } catch (SQLException e) {
            logger.error("Error while creating table.", e);
        }
    }

    public static Result getFinalCounter() {
        try (Connection conn = ConnectionUtils.getConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet resultSet = stmt.executeQuery("SELECT counter, version FROM user_counter WHERE user_id = 1 FOR  UPDATE");
            resultSet.next();

            int counter = resultSet.getInt("counter");
            int version = resultSet.getInt("version");

            return new Result(counter, version);
        } catch (SQLException e) {
            logger.error("Error while getting counter.", e);
            throw new RuntimeException(e);
        }
    }
}
