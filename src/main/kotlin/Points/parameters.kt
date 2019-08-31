package Points

import java.util.UUID
import java.sql.Timestamp

interface TransactionParameters;

data class AddUserParameters(val userId: UUID, val total: Int) : TransactionParameters
data class AddPointsParameters(val userId: UUID, val amount: Int) : TransactionParameters
data class GetUserInfoParameters(val userId: UUID) : TransactionParameters
data class GetTransactionInfoParameters(val transactionId: Long) : TransactionParameters
data class UpdateTemporaryPointsParameters(val userId: UUID, val updateTime: Timestamp = Timestamp(System.currentTimeMillis())) : TransactionParameters
data class AddTemporaryPointsParameters(val userId: UUID, val amount: Int, val expiryDate: Timestamp) : TransactionParameters
data class ReservePointsParameters(val userId: UUID, val amount: Int) : TransactionParameters
data class CancelTransactionParameters(val transactionId: Long) : TransactionParameters
data class WriteOffPointsParameters(val transactionId: Long) : TransactionParameters
