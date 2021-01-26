Hello,

This is a sample for AWS QLDB, it illustrates some basic operations and how to handle conflicts natively.

## Get the sample running

- Set up the AWS CLI
- Install JDK (8 or above)
- Inside the project folder run "./gradlew bootRun"
- Browse to http://localhost:8080/swagger.html

## Exposed APIs

- ReadMeFirst: Gives a brief about this sample.
- CreateAccountsTable: Creates Accounts table in the ledger.
- DropAccountsTable: Drops Accounts table from the ledger.
- CreateTransactionsTable: Creates Transactions table in the ledger.
- DropTransactionsTable: Drops Transactions table from the ledger.
- InsertBuyerAndSeller: Inserts buyer and seller documents to be used through the sample testing.
- ListAccounts: Lists all documents in the Accounts table (Accounts list will be printed to the console).
- DeleteAllAccounts: Deletes all documents from Accounts table.



- ListTransactions: Lists all documents in the Transactions table (Transactions list will be printed to the console).
- DeleteAllTransactions: Deletes all documents from Transactions table.  


- BalanceTransfer: Carries out a single threaded balance transfer.
- BalanceTransferConflict: Carries out a multi threaded balance transfer (they are conflicting).

## Notes

- All the project code exists in class "BankManager"
- All the system operations steps are to the console

Mahmoud