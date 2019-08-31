package Points;

import java.util.UUID;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;

import java.sql.DriverManager;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.sql.Timestamp;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DatabaseDriver {
    public DatabaseDriver(@NotNull Logger parentLogger) {
        logger.setParent(parentLogger);
        logger.setLevel(null);
    }

    public DatabaseDriver() {
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.ALL);
        logger.addHandler(consoleHandler);
    }

    private static final String DBADRESS = "jdbc:mysql://localhost/Points?" +
                                           "user=points_driver&" +
                                           "password=points_password&" +
                                           "useJDBCCompliantTimezoneShift=true&" +
                                           "useLegacyDatetimeCode=false&" +
                                           "serverTimezone=UTC";

    private static Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(DBADRESS);
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        return connection;
    }

    private <T extends Exception> void logAndThrow(@NotNull T ex) throws T {
        logger.severe​(ex.getMessage());
        throw ex;
    }

    private static final String ADD_USER_QUERY = "INSERT INTO Points.points " +
                                                 "(user_id, total, total_temporary) " +
                                                 "VALUES (UUID_TO_BIN(?), ?, ?)";
    private static final String ADD_USER_QUERY_HISTORY = "INSERT INTO transaction " +
                                                         "(user_id, amount, action)" +
                                                         "VALUES(UUID_TO_BIN(?), ?, \"add user\")";
    public void addUser(@NotNull AddUserParameters parameters) throws SQLException {
        String userIdString = parameters.getUserId().toString();
        logger.fine("Adding a new user " + userIdString + "...");
        try(Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(ADD_USER_QUERY);
            PreparedStatement statementHistory = connection.prepareStatement(ADD_USER_QUERY_HISTORY)) {
            statement.setString(1, userIdString);
            statement.setInt(2, parameters.getTotal());
            statement.setInt(3, 0);
            statement.executeUpdate();

            statementHistory.setString(1, userIdString);
            statementHistory.setInt(2, parameters.getTotal());
            statementHistory.executeUpdate();

            connection.commit();
            logger.fine("New user " + userIdString + " succefully added!");
        } catch (SQLIntegrityConstraintViolationException ex) {
            if (ex.getMessage().startsWith("Duplicate entry '")) {
                logger.warning("Attempt to add already added user " + userIdString + "!!");
            } else {
                logAndThrow(ex);
            }
        } catch (SQLException ex) {
            logAndThrow(ex);
        }
    }

    private static final String ADD_POINTS_QUERY = "UPDATE Points.points " +
                                                   "SET total = total + ? " +
                                                   "WHERE user_id = UUID_TO_BIN(?)";
    private static final String ADD_POINTS_QUERY_HISTORY = "INSERT INTO transaction " +
                                                           "(user_id, amount, action)" +
                                                           "VALUES(UUID_TO_BIN(?), ?, \"add points\")";
    public void addPoints(@NotNull AddPointsParameters parameters) throws SQLException {
        String userIdString = parameters.getUserId().toString();
        logger.fine("Adding points " + parameters.getAmount() + " to " + userIdString + "...");

        GetUserInfoReturn userInfo = getUserInfo(parameters.getUserId());
        if (userInfo == null) {
            logger.warning("Attempt to add points to invalid user " + userIdString + "!!");
            return;
        }

        try(Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(ADD_POINTS_QUERY);
            PreparedStatement statementHistory = connection.prepareStatement(ADD_POINTS_QUERY_HISTORY)) {
            statement.setInt(1, parameters.getAmount());
            statement.setString(2, userIdString);
            statement.executeUpdate();

            statementHistory.setString(1, userIdString);
            statementHistory.setInt(2, parameters.getAmount());
            statementHistory.executeUpdate();

            connection.commit();
            logger.fine("" + parameters.getAmount() + " points successfully added to " + userIdString + "!");
        } catch (SQLException ex) {
            logAndThrow(ex);
        }
    }

    private static final String GET_USER_INFO_QUERY = "SELECT total, " +
                                                              "total_temporary, " +
                                                              "payed_temporary, " +
                                                              "reserved, " +
                                                              "earliest_expiry_date, " +
                                                              "earliest_expiry_amount " +
                                                      "FROM Points.points " +
                                                      "WHERE user_id = UUID_TO_BIN(?)";
    private GetUserInfoReturn getUserInfo(@NotNull UUID userId) throws SQLException {
        logger.fine("Requesting info of user " + userId.toString() + "...");

        try(Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(GET_USER_INFO_QUERY)) {
            statement.setString(1, userId.toString());
            while (true) {
                try(ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        logger.warning("Requested user " + userId.toString() + " is invalid!!");
                        return null;
                    }

                    Timestamp earliestExpiryDate = result.getTimestamp(5);
                    if (earliestExpiryDate == null
                        || earliestExpiryDate.before(new Timestamp(System.currentTimeMillis()))) {
                        int total = result.getInt(1);
                        int totalTemporary = result.getInt(2);
                        int payedTemporary = result.getInt(3);
                        int reserved = result.getInt(4);
                        int earliestExpiryAmount = result.getInt(6);
                        logger.fine("Requesting info of user " + userId.toString() + " is successful!");
                        return new GetUserInfoReturn(userId,
                                                     total,
                                                     totalTemporary,
                                                     payedTemporary,
                                                     reserved,
                                                     earliestExpiryDate,
                                                     earliestExpiryAmount);
                    } else {
                        logger.warning("Requesting user " + userId.toString() + " needs update, waiting...");
                        updateTemporaryPoints(userId);
                        continue;
                    }
                }
            }
        } catch (SQLException ex) {
            logAndThrow(ex);
            throw new RuntimeException("Unreacheable code!");
        }
    }

    public GetUserInfoReturn getUserInfo(@NotNull GetUserInfoParameters parameters) throws SQLException {
        return getUserInfo(parameters.getUserId());
    }

    private static final String GET_TRANSACTION_INFO_QUERY = "SELECT BIN_TO_UUID(user_id), " +
                                                                    "amount, " +
                                                                    "time, " +
                                                                    "expiry_time, " +
                                                                    "action " +
                                                              "FROM Points.transaction " +
                                                              "WHERE id = ?";
    private GetTransactionInfoReturn getTransactionInfo(long transactionId) throws SQLException {
        logger.fine("Requesting info of transaction " + transactionId + "...");
        try(Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(GET_TRANSACTION_INFO_QUERY)) {
            statement.setLong(1, transactionId);
            try(ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    logger.warning("Requested transaction " + transactionId + " is invalid!!");
                    return null;
                }

                UUID userId = UUID.fromString(result.getString(1));
                int amount = result.getInt(2);
                Timestamp time = result.getTimestamp(3);
                Timestamp expiryTime = result.getTimestamp(4);
                String action = result.getString(5);

                logger.fine("Requesting info of transaction " + transactionId + " is successful!");
                return new GetTransactionInfoReturn(transactionId,
                                                    userId,
                                                    amount,
                                                    time,
                                                    expiryTime,
                                                    action);
            }
        } catch (SQLException ex) {
            logAndThrow(ex);
            throw new RuntimeException("Unreacheable code!");
        }
    }

    public GetTransactionInfoReturn getTransactionInfo(@NotNull GetTransactionInfoParameters parameters) throws SQLException {
        return getTransactionInfo(parameters.getTransactionId());
    }

    private static final String UPDATE_TEMPORARY_POINTS_SELECT_DELETED = "SELECT amount " +
                                                                         "FROM Points.temporary_points " +
                                                                         "WHERE user_id = UUID_TO_BIN(?) and expiry_time < ?";
    private static final String UPDATE_TEMPORARY_POINTS_NEW = "SELECT amount, expiry_time " +
                                                              "FROM Points.temporary_points " +
                                                              "WHERE user_id = UUID_TO_BIN(?) and expiry_time >= ? " +
                                                              "ORDER BY expiry_time " +
                                                              "LIMIT 1";
    private static final String UPDATE_TEMPORARY_POINTS = "UPDATE Points.points " +
                                                          "SET total_temporary = ?, " +
                                                              "payed_temporary = ?, " +
                                                              "earliest_expiry_date = ?, " +
                                                              "earliest_expiry_amount = ? " +
                                                          "WHERE user_id = UUID_TO_BIN(?)";
    private static final String UPDATE_TEMPORARY_POINTS_DELETE = "DELETE FROM Points.temporary_points " +
                                                                 "WHERE user_id = UUID_TO_BIN(?) and expiry_time < ?";
    private void updateTemporaryPoints(@NotNull UUID userId, @NotNull Timestamp now) throws SQLException {
        logger.fine("Updating temporary points of user " + userId.toString() + "...");

        GetUserInfoReturn userInfo = getUserInfo(userId);
        if (userInfo == null) {
            logger.warning("Attempt to update temporary points of an invalid user " + userId.toString() + "!!");
            return;
        }

        if (userInfo.getEarliestExpiryDate() == null
            || userInfo.getEarliestExpiryDate().after(now)) {
            logger.fine("User " + userId.toString() + " temporary points are up to date!");
            return;
        }
        int total = userInfo.getTotal();
        int totalTemporary = userInfo.getTotalTemporary();
        int payedTemporary = userInfo.getPayedTemporary();
        try(Connection connection = getConnection();
            PreparedStatement statementSelect = connection.prepareStatement(UPDATE_TEMPORARY_POINTS_SELECT_DELETED);
            PreparedStatement statementSelectNew = connection.prepareStatement(UPDATE_TEMPORARY_POINTS_NEW);
            PreparedStatement statement = connection.prepareStatement(UPDATE_TEMPORARY_POINTS);
            PreparedStatement statementDelete = connection.prepareStatement(UPDATE_TEMPORARY_POINTS_DELETE)) {

            statementSelect.setString(1, userId.toString());
            statementSelect.setTimestamp(2, now);
            try (ResultSet resultSelect = statementSelect.executeQuery()) {
                while (resultSelect.next()) {
                    int amount = resultSelect.getInt(1);
                    if (payedTemporary >= amount) {
                        payedTemporary -= amount;
                    } else {
                        if (payedTemporary > 0) {
                            amount -= payedTemporary;
                            payedTemporary = 0;
                        }
                        totalTemporary -= amount;
                    }
                }
            }

            Timestamp earliestExpiryDate = null;
            int earliestExpiryAmount = 0;

            statementSelectNew.setString(1, userId.toString());
            statementSelectNew.setTimestamp(2, now);
            try (ResultSet resultSelectNew = statementSelectNew.executeQuery()) {
                if (resultSelectNew.next()) {
                    earliestExpiryAmount = resultSelectNew.getInt(1);
                    earliestExpiryDate = resultSelectNew.getTimestamp(2);
                }
            }

            statement.setInt(1, totalTemporary);
            statement.setInt(2, payedTemporary);
            statement.setTimestamp(3, earliestExpiryDate);
            statement.setInt(4, earliestExpiryAmount);
            statement.setString(5, userId.toString());
            statement.executeUpdate();

            statementDelete.setString(1, userId.toString());
            statementDelete.setTimestamp(2, now);
            statementDelete.executeUpdate();

            connection.commit();
            logger.fine("User " + userId.toString() + " temporary points successfully updated!");
        } catch (SQLException ex) {
            logAndThrow(ex);
        }
    }

    public void updateTemporaryPoints(@NotNull UpdateTemporaryPointsParameters parameters) throws SQLException {
        updateTemporaryPoints(parameters.getUserId(), parameters.getUpdateTime());
    }

    private void updateTemporaryPoints(@NotNull UUID userId) throws SQLException {
        updateTemporaryPoints(userId, new Timestamp(System.currentTimeMillis()));
    }

    private static final String ADD_TEMPORARY_POINTS_HISTORY = "INSERT INTO Points.transaction " +
                                                               "(user_id, amount, expiry_time, action) " +
                                                               "VALUES (UUID_TO_BIN(?), ?, ?, \"temporary points addition\")";
    private static final String ADD_TEMPORARY_POINTS_TEMP = "INSERT INTO Points.temporary_points " +
                                                            "(user_id, transaction_id, amount, expiry_time) " +
                                                            "VALUES (UUID_TO_BIN(?), ?, ?, ?)";
    private static final String ADD_TEMPORARY_POINTS_USER = "UPDATE Points.points " +
                                                            "SET total_temporary = total_temporary + ?, " +
                                                                "earliest_expiry_date = ?, " +
                                                                "earliest_expiry_amount = ? " +
                                                            "WHERE user_id = UUID_TO_BIN(?)";
    public void addTemporaryPoints(@NotNull AddTemporaryPointsParameters parameters) throws SQLException {
        String userIdString = parameters.getUserId().toString();
        logger.fine("Adding temporary points " + parameters.getAmount() + " to user " + userIdString + "...");

        GetUserInfoReturn userInfo = getUserInfo(parameters.getUserId());
        if (userInfo == null) {
            logger.warning("Attempt to add temporary points to an invalid user " + userIdString + "!!");
            return;
        }
        try(Connection connection = getConnection();
            PreparedStatement statementHistory = connection.prepareStatement(ADD_TEMPORARY_POINTS_HISTORY, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement statementTemporary = connection.prepareStatement(ADD_TEMPORARY_POINTS_TEMP);
            PreparedStatement statementUsers = connection.prepareStatement(ADD_TEMPORARY_POINTS_USER)) {
            statementHistory.setString(1, parameters.getUserId().toString());
            statementHistory.setInt(2, parameters.getAmount());
            statementHistory.setTimestamp(3, parameters.getExpiryDate());
            statementHistory.executeUpdate();

            long transactionId = -1;
            try(ResultSet resultHistory = statementHistory.getGeneratedKeys()) {
                Boolean hasAnswer = resultHistory.next();
                assert hasAnswer;
                transactionId = resultHistory.getLong(1);
            }

            statementTemporary.setString(1, parameters.getUserId().toString());
            statementTemporary.setLong(2, transactionId);
            statementTemporary.setInt(3, parameters.getAmount());
            statementTemporary.setTimestamp(4, parameters.getExpiryDate());
            statementTemporary.executeUpdate();

            Timestamp earliestExpiryDate = parameters.getExpiryDate();
            int earliestExpiryAmount = parameters.getAmount();
            if (userInfo.getEarliestExpiryDate() != null
                && userInfo.getEarliestExpiryDate().before(earliestExpiryDate)) {
                earliestExpiryDate = userInfo.getEarliestExpiryDate();
                earliestExpiryAmount = userInfo.getEarliestExpiryAmount();
            }

            statementUsers.setInt(1, parameters.getAmount());
            statementUsers.setTimestamp(2, earliestExpiryDate);
            statementUsers.setInt(3, earliestExpiryAmount);
            statementUsers.setString(4, parameters.getUserId().toString());
            statementUsers.executeUpdate();

            connection.commit();
            logger.fine("Added temporary points " + parameters.getAmount() + " to user " + userIdString + "!");
        } catch (SQLException ex) {
            logAndThrow(ex);
        }
    }

    private static final String RESERVE_POINTS_QUERY_HISTORY = "INSERT INTO transaction " +
                                                               "(user_id, amount, time, action)" +
                                                               "VALUES(UUID_TO_BIN(?), ?, ?, \"reserve\")";
    private static final String RESERVE_POINTS_QUERY_USER = "UPDATE Points.points " +
                                                            "SET reserved = ? " +
                                                            "WHERE user_id = UUID_TO_BIN(?)";
    public ReservePointsReturn reservePoints(@NotNull ReservePointsParameters parameters) throws SQLException {
        String userIdString = parameters.getUserId().toString();
        logger.fine("Reserving points " + parameters.getAmount() + " of user " + userIdString + "...");

        GetUserInfoReturn userInfo = getUserInfo(parameters.getUserId());
        if (userInfo == null) {
            logger.warning("Attempt to reserve points of an invalid user " + userIdString + "!!");
            return null;
        }

        updateTemporaryPoints(parameters.getUserId());

        if (userInfo.getTotal() + userInfo.getTotalTemporary() < userInfo.getReserved() + parameters.getAmount()) {
            logger.warning("Selected user " + userIdString + " does not have points enough (total: " + userInfo.getTotal() + ", temporary: " + userInfo.getTotalTemporary() + ", reserved: " + userInfo.getReserved() + ") to make a reservation of " + parameters.getAmount() + "!!");
            return new ReservePointsReturn(-1);
        }
        try(Connection connection = getConnection();
            PreparedStatement statementHistory = connection.prepareStatement(RESERVE_POINTS_QUERY_HISTORY, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement statementUsers = connection.prepareStatement(RESERVE_POINTS_QUERY_USER)) {
            statementHistory.setString(1, parameters.getUserId().toString());
            statementHistory.setInt(2, parameters.getAmount());
            statementHistory.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            statementHistory.executeUpdate();

            long transactionId = -1;
            try(ResultSet result_history = statementHistory.getGeneratedKeys()) {
                if (!result_history.next()) {
                    //TODO: assert
                }
                transactionId = result_history.getLong(1);
            }

            statementUsers.setInt(1, userInfo.getReserved() + parameters.getAmount());
            statementUsers.setString(2, userIdString);
            statementUsers.executeUpdate();

            connection.commit();
            logger.fine("Reserved successfully points " + parameters.getAmount() + " of user " + userIdString + " in transaction " + transactionId + "!");
            return new ReservePointsReturn(transactionId);
        } catch (SQLException ex) {
            logAndThrow(ex);
            throw new RuntimeException("Unreacheable code!");
        }
    }

    private static final String CANCEL_TRANSACTION_QUERY_HISTORY = "UPDATE transaction " +
                                                                   "SET action = \"canceled\"" +
                                                                   "WHERE id = ?";
    private static final String CANCEL_TRANSACTION_QUERY_USER = "UPDATE Points.points " +
                                                                "SET reserved = ? " +
                                                                "WHERE user_id = UUID_TO_BIN(?)";
    public void cancelTransaction(long transactionId) throws SQLException {
        logger.fine("Canceling transaction " + transactionId + "...");
        GetTransactionInfoReturn transactionInfo = getTransactionInfo(transactionId);
        if (transactionInfo == null) {
            logger.warning("Attempt to cancel an invalid transaction " + transactionId + "!!");
            return;
        }
        if (transactionInfo.getAction().equals("committed")) {
            logger.severe("Canceling committed transaction " + transactionId + "!!");
            throw new SQLException("Canceling committed transaction!!");
        } else if (transactionInfo.getAction().equals("canceled")) {
            logger.warning("Canceling already canceled transaction " + transactionId + "!");
            return;
        } else if (!transactionInfo.getAction().equals("reserve")) {
            logger.severe("Canceling transaction " + transactionId + " of wrong type!!");
            throw new SQLException("Wrong type transaction calcelation!!");
        }

        try(Connection connection = getConnection();
            PreparedStatement statementHistory = connection.prepareStatement(CANCEL_TRANSACTION_QUERY_HISTORY);
            PreparedStatement statementUser = connection.prepareStatement(CANCEL_TRANSACTION_QUERY_USER)) {

            statementHistory.setLong(1, transactionId);
            statementHistory.executeUpdate();

            GetUserInfoReturn userInfo = getUserInfo(transactionInfo.getUserId());
            int reserved = userInfo.getReserved();
            reserved -= transactionInfo.getAmount();

            statementUser.setInt(1, reserved);
            statementUser.setString(2, transactionInfo.getUserId().toString());
            statementUser.executeUpdate();

            connection.commit();
            logger.fine("Canceled transaction " + transactionId + " successfully!");
        } catch (SQLException ex) {
            logAndThrow(ex);
        }
    }

    public void cancelTransaction(@NotNull CancelTransactionParameters parameters) throws SQLException {
        cancelTransaction(parameters.getTransactionId());
    }

    private static final String WRITE_OFF_QUERY_HISTORY = "UPDATE transaction " +
                                                          "SET action = \"committed\"" +
                                                          "WHERE id = ?";
    private static final String WRITE_OFF_QUERY_USER = "UPDATE Points.points " +
                                                       "SET total = ?, " +
                                                       "total_temporary = ?, " +
                                                       "payed_temporary = ?, " +
                                                       "reserved = ? " +
                                                       "WHERE user_id = UUID_TO_BIN(?)";
    public void writeOffPoints(long transactionId) throws SQLException {
        logger.fine("Writing off points from transaction " + transactionId + "...");

        GetTransactionInfoReturn transactionInfo = getTransactionInfo(transactionId);
        if (transactionInfo == null) {
            logger.warning("Attempt to commit an invalid transaction " + transactionId + "!!");
            return;
        }

        if (transactionInfo.getAction().equals("committed")) {
            logger.warning("Commiting already committed transaction " + transactionId + "!");
            return;
        } else if (transactionInfo.getAction().equals("canceled")) {
            logger.severe("Commiting canceled transaction " + transactionId + "!!");
            throw new SQLException("Commiting canceled transaction!!");
        } else if (!transactionInfo.getAction().equals("reserve")) {
            logger.severe("Commiting transaction " + transactionId + " of wrong type!!");
            throw new SQLException("Wrong transaction commiting!!");
        }
        updateTemporaryPoints(transactionInfo.getUserId());

        GetUserInfoReturn userInfo = getUserInfo(transactionInfo.getUserId());
        if (userInfo.getTotal() + userInfo.getTotalTemporary() < transactionInfo.getAmount()) {
            logger.warning("Selected user " + userInfo.getUserId().toString() + " does not have points enough (total: " + userInfo.getTotal() + ", temporary: " + userInfo.getTotalTemporary() + ", reserved: " + userInfo.getReserved() + ") to commit transaction " + transactionId + ", that requires " + transactionInfo.getAmount() + " points!!");
            cancelTransaction(transactionId);
        } else {
            int amount = transactionInfo.getAmount();
            int total = userInfo.getTotal();
            int totalTemporary = userInfo.getTotalTemporary();
            int payedTemporary = userInfo.getPayedTemporary();
            int reserved = userInfo.getReserved();
            if (amount < totalTemporary) {
                totalTemporary -= amount;
                payedTemporary += amount;
            } else if (totalTemporary == 0) {
                total -= amount;
                assert total >= 0;
            } else {
                total -= amount - totalTemporary;
                totalTemporary = 0;
                payedTemporary += totalTemporary;
            }
            try(Connection connection = getConnection();
                PreparedStatement statementHistory = connection.prepareStatement(WRITE_OFF_QUERY_HISTORY);
                PreparedStatement statementUser = connection.prepareStatement(WRITE_OFF_QUERY_USER)) {
                statementHistory.setLong(1, transactionId);
                statementHistory.executeUpdate();

                statementUser.setInt(1, total);
                statementUser.setInt(2, totalTemporary);
                statementUser.setInt(3, payedTemporary);
                statementUser.setInt(4, reserved);
                statementUser.setString(5, userInfo.getUserId().toString());
                statementUser.executeUpdate();

                connection.commit();
                logger.fine("Committed transaction " + transactionId + " successfully!");
            } catch (SQLException ex) {
                logAndThrow(ex);
            }
        }

    }

    public void writeOffPoints(@NotNull WriteOffPointsParameters parameters) throws SQLException {
        writeOffPoints(parameters.getTransactionId());
    }

    private Logger logger = Logger.getLogger​("DatabaseDriver");
}
