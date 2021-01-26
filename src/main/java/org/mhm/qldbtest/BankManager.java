package org.mhm.qldbtest;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonSystemBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.RetryPolicy;

@RestController
public class BankManager {

    static {
        System.out.println("Initializing the driver");
        System.setProperty("aws.region", "eu-west-1");
        qldbDriver = QldbDriver.builder()
                .ledger("mahmoudabdelsattar-ledger") //replace this with your ledger
                .transactionRetryPolicy(RetryPolicy
                        .builder()
                        .maxRetries(3)
                        .build())
                .sessionClientBuilder(QldbSessionClient.builder())
                .build();
    }

    public static QldbDriver qldbDriver;
    public static IonSystem ionSys = IonSystemBuilder.standard().build();

    @GetMapping("/ReadMeFirst")
    public ResponseEntity<String> greet() {
        return new ResponseEntity<String>("First click this then that!\nfinally those", HttpStatus.OK);
    }

    @GetMapping("/CreateAccountsTable")
    public ResponseEntity<String> createAccountsTable() {
        qldbDriver.execute(txn -> {
            System.out.println("Creating Accounts Table and its Index");
            txn.execute("CREATE TABLE Accounts");
            txn.execute("CREATE INDEX ON Accounts(accountID)");
        });
        return new ResponseEntity<String>("Table Accounts Created", HttpStatus.OK);
    }

    @GetMapping("/DropAccountsTable")
    public ResponseEntity<String> dropAccountsTable() {
        qldbDriver.execute(txn -> {
            System.out.println("Dropping Accounts Table");
            txn.execute("DROP TABLE Accounts");
        });
        return new ResponseEntity<String>("Table Accounts Dropped", HttpStatus.OK);
    }

    @GetMapping("/CreateTransactionsTable")
    public ResponseEntity<String> createTransactionsTable() {
        qldbDriver.execute(txn -> {
            System.out.println("Creating Transactions Table");
            txn.execute("CREATE TABLE Transactions");
        });
        return new ResponseEntity<String>("Table Transactions Created", HttpStatus.OK);
    }

    @GetMapping("/DropTransactionsTable")
    public ResponseEntity<String> dropTransactionsTable() {
        qldbDriver.execute(txn -> {
            System.out.println("Dropping Transactions Table");
            txn.execute("DROP TABLE Transactions");
        });
        return new ResponseEntity<String>("Table Transactions Dropped", HttpStatus.OK);
    }

    @GetMapping("/InsertBuyerAndSeller")
    public ResponseEntity<String> insertBuyerAndSeller() {
        qldbDriver.execute(txn -> {
            System.out.println("Inserting a Buyer");
            IonStruct buyer = ionSys.newEmptyStruct();
            buyer.put("accountID").newInt(1);
            buyer.put("accountType").newString("Buyer");
            buyer.put("balance").newInt(500);
            txn.execute("INSERT INTO Accounts ?", buyer);

            System.out.println("Inserting a Seller");
            IonStruct seller = ionSys.newEmptyStruct();
            seller.put("accountID").newInt(2);
            seller.put("accountType").newString("Seller");
            seller.put("balance").newInt(100);
            txn.execute("INSERT INTO Accounts ?", seller);
        });
        return new ResponseEntity<String>("A Buyer and a Seller were Created", HttpStatus.OK);
    }

    @GetMapping("ListAccounts")
    public static ResponseEntity<String> listAccounts() {
        qldbDriver.execute(txn -> {
            System.out.println("List All Accounts");
            Result result = txn.execute("SELECT * FROM Accounts");
            result.forEach(account -> {
                System.out.println(account.toPrettyString());
            });
        });
        return new ResponseEntity<>("Check the console for the accounts list", HttpStatus.OK);
    }

    @GetMapping("DeleteAllAccounts")
    public ResponseEntity<String> deleteAllAccounts() {
        qldbDriver.execute(txn -> {
            System.out.println("Delete All Accounts");
            Result result = txn.execute("DELETE FROM Accounts");
        });
        return new ResponseEntity<>("All Accounts were Deleted", HttpStatus.OK);
    }

    @GetMapping("ListTransactions")
    public static ResponseEntity<String> listTransactions() {
        qldbDriver.execute(txn -> {
            System.out.println("List All Transactions");
            Result result = txn.execute("SELECT * FROM Transactions");
            result.forEach(transaction -> {
                System.out.println(transaction.toPrettyString());
            });
        });
        return new ResponseEntity<>("Check the console for the Transactions list", HttpStatus.OK);
    }

    @GetMapping("DeleteAllTransactions")
    public ResponseEntity<String> deleteAllTransactions() {
        qldbDriver.execute(txn -> {
            System.out.println("Delete All Transactions");
            Result result = txn.execute("DELETE FROM Transactions");
        });
        return new ResponseEntity<>("All Transactions were Deleted", HttpStatus.OK);
    }

    @PostMapping("/BalanceTransfer")
    public ResponseEntity<String> balanceTransfer(@RequestParam(defaultValue = "Single Transaction", required = false) String transactionName,
                                                  @RequestParam(defaultValue = "1", required = false) Integer fromAccount,
                                                  @RequestParam(defaultValue = "2", required = false) Integer toAccount,
                                                  @RequestParam(defaultValue = "10", required = false) Integer transferAmount,
                                                  @RequestParam(defaultValue = "1000", required = false) Integer delay) {
        return new ResponseEntity<>(balanceTransferWithDelay(transactionName, fromAccount, toAccount, transferAmount, delay), HttpStatus.OK);
    }

    @PostMapping("/BalanceTransferConflict")
    public ResponseEntity<String> balanceTransferConflict() {
        System.out.println("First Transaction Starting");
        balanceTransferWithDelay("First Transaction", 1, 2, 20, 10000);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Second Transaction Starting");
        balanceTransferWithDelay("Second Transaction", 1, 2, 10, 2000);
        //System.out.println("Please Wait 30 Second");
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        listAccounts();
        listTransactions();
        return new ResponseEntity<>("Conflict Happened", HttpStatus.OK);
    }

    public static String balanceTransferWithDelay(String transactionName, Integer fromAccount, Integer toAccount, Integer transferAmount, Integer delay) {
        new Thread(() -> {
            try {
                qldbDriver.execute(txn -> {
                    System.out.println(transactionName + " Load Buyer Balance");
                    Result buyerResult = txn.execute("SELECT * FROM Accounts WHERE accountID = ?", ionSys.newInt(fromAccount));
                    IonStruct buyer = (IonStruct) buyerResult.iterator().next();
                    Integer buyerBalance = Integer.parseInt(buyer.get("balance").toString());
                    System.out.println(transactionName + " Buyer Balance : " + buyerBalance);

                    System.out.println(transactionName + " Load Seller Balance");
                    Result sellerResult = txn.execute("SELECT * FROM Accounts WHERE accountID = ?", ionSys.newInt(toAccount));
                    IonStruct seller = (IonStruct) sellerResult.iterator().next();
                    Integer sellerBalance = Integer.parseInt(seller.get("balance").toString());
                    System.out.println(transactionName + " Buyer Balance : " + sellerBalance);

                    if (buyerBalance < transferAmount) {
                        System.out.println(transactionName + " Not Enough Balance");
                        txn.abort();
                        return;
                    }

                    System.out.println(transactionName + " Sleep for : " + delay + " milliseconds!");
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Integer updatedBuyerBalance = buyerBalance - transferAmount;
                    Integer updatedSellerBalance = sellerBalance + transferAmount;

                    System.out.println(transactionName + " Update Buyer Balance, New Balance : " + updatedBuyerBalance);
                    txn.execute("UPDATE Accounts SET balance = ? WHERE accountID = ?", ionSys.newInt(updatedBuyerBalance), ionSys.newInt(fromAccount));
                    System.out.println(transactionName + " Update Seller Balance, New Balance : " + updatedSellerBalance);
                    txn.execute("UPDATE Accounts SET balance = ? WHERE accountID = ?", ionSys.newInt(updatedSellerBalance), ionSys.newInt(toAccount));
                    System.out.println(transactionName + " Insert Transaction : " + transactionName);
                    IonStruct transaction = ionSys.newEmptyStruct();
                    transaction.put("transactionName").newString(transactionName);
                    transaction.put("from").newInt(fromAccount);
                    transaction.put("to").newInt(toAccount);
                    transaction.put("amount").newInt(transferAmount);
                    transaction.put("timestamp").newTimestamp(Timestamp.now());
                    txn.execute("INSERT INTO Transactions ?", transaction);

                });
            } catch (OccConflictException e) {
                System.out.println("Occ Conflict Exception");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        return "Balance Transfer Done after delay : " + delay;
    }
}