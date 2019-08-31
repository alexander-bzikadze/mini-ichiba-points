package Points;

import java.util.UUID;
import java.util.logging.Logger;
import java.util.logging.Handler;

import java.sql.SQLException;
import java.sql.Timestamp;

public class App {
    public String getGreeting() {
        return "App greets you!";
    }

    public static void main(String[] args) throws SQLException {
        // DatabaseInitializer databaseInitializer = new DatabaseInitializer();
        DatabaseDriver driver = new DatabaseDriver();

        UUID uuid = UUID.nameUUIDFromBytes(new byte[0]);
        UUID uuid_inv = UUID.nameUUIDFromBytes(new byte[]{1});

        driver.addUser(new AddUserParameters(uuid, 0));

        driver.addPoints(new AddPointsParameters(uuid, 1));
        driver.addPoints(new AddPointsParameters(uuid, 2));
        driver.addPoints(new AddPointsParameters(uuid_inv, 2));

        driver.addTemporaryPoints(new AddTemporaryPointsParameters(uuid, 1, new Timestamp(2_000_000)));
        driver.updateTemporaryPoints(new UpdateTemporaryPointsParameters(uuid, new Timestamp(System.currentTimeMillis())));

        long transactionId = driver.reservePoints(new ReservePointsParameters(uuid, 1)).getTransactionId();
        driver.cancelTransaction(new CancelTransactionParameters(transactionId));
        driver.cancelTransaction(new CancelTransactionParameters(transactionId));

        transactionId = driver.reservePoints(new ReservePointsParameters(uuid, 1)).getTransactionId();
        driver.writeOffPoints(new WriteOffPointsParameters(transactionId));
        driver.writeOffPoints(new WriteOffPointsParameters(transactionId));
    }
}
