package Points

import java.util.UUID
import java.sql.Timestamp

interface TransactionReturn;

data class GetUserInfoReturn(val userId: UUID, val total: Int, val totalTemporary: Int, val payedTemporary: Int, val reserved: Int, val earliestExpiryDate: Timestamp?, val earliestExpiryAmount: Int) : TransactionReturn
data class ReservePointsReturn(val transactionId: Long) : TransactionReturn
data class GetTransactionInfoReturn(val transactionId: Long, val userId: UUID, val amount: Int, val time: Timestamp, val expiry_time: Timestamp?, val action: String) : TransactionReturn
