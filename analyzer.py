import gzip
import pandas as pd
import re
import os
import argparse

# Output directory for the results
OUTPUT_DIR = "output"


class CaloLogAnalyzer:

    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.raw_logs = None
        self.parsed_logs = None
        self.transactions = None
        self.analysis_results = {}
        # Create the output directory if it doesn't exist
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

    def load_logs(self):
        """
        This function helps in loading the log data from the input path
        and return them as a Pandas dataframe.
        """
        print(f"\n-> Loading logs from {self.log_file_path}...")

        all_log_lines = []

        # go through the log_directory and all of its subfolders
        for root, dirs, files in os.walk(self.log_file_path):
            # Loop through the files found in the current folder
            for filename in files:
                if filename.endswith(".gz"):
                    file_path = os.path.join(root, filename)
                    try:
                        with gzip.open(file_path, "rt", encoding="utf-8") as f:
                            for line in f:
                                if "INFO" in line:
                                    all_log_lines.append(line.strip())
                    except Exception as e:
                        print(f"Error reading {file_path}: {e}")

        # Create the DataFrame
        self.raw_logs = pd.DataFrame(all_log_lines, columns=["raw_log"])

        # self.raw_logs = pd.read_csv(self.log_file_path)
        print(f"   Raw logs loaded. Total count: {len(self.raw_logs):,}")

    def parse_log_data(self, string_data):
        """
        This function helps in parsing the individual string message and
        convert / captures the individual details such as timestamp, session_id,
        message_type and message.

        """
        if pd.isna(string_data):
            return None
        log_data = str(string_data).strip()
        log_data_list = log_data.split("\t")
        timestamp_match = re.match(
            r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)", log_data
        )
        timestamp = timestamp_match.group(1) if timestamp_match else None
        return {
            "timestamp": timestamp,
            "session_id": log_data_list[1] if len(log_data_list) > 1 else None,
            "message_type": log_data_list[2] if len(log_data_list) > 2 else None,
            "message": log_data_list[3] if len(log_data_list) > 3 else None,
        }

    def parse_all_logs(self):
        """
        This function helps in reading the dataframe and iterates through each row
        and captures the idividual message details, and returns a new dataframe.

        """
        print("-> Parsing all log entries...")
        parsed_data = []

        for idx, row in self.raw_logs.iterrows():
            parsed = self.parse_log_data(row["raw_log"])
            if parsed:
                parsed_data.append(parsed)
        # single_log_entry = raw_logs.iloc[43]['raw_log']
        # parsed = parse_log_data(single_log_entry)
        # if parsed:
        #     parsed_data.append(parsed)
        self.parsed_logs = pd.DataFrame(parsed_data)
        # Convert timestamp to datetime
        self.parsed_logs["datetime"] = pd.to_datetime(self.parsed_logs["timestamp"])
        return self.parsed_logs

    def categorize_logs(self):
        """
        This function helps in categorizing the messages captured based on the
        identified keywords

        """
        print("\n-> Categorizing log messages...")

        def categorize_message(msg):
            if pd.isna(msg):
                return "other"
            msg_lower = str(msg).lower()

            # print(msg_lower)

            if "processing message" in msg_lower:
                return "processing_message"
            elif "start syncing the balance" in msg_lower:
                return "balance_sync_start"
            elif "balance is already synced" in msg_lower:
                return "balance_already_synced"
            elif "skipping the balance sync" in msg_lower:
                return "balance_sync_skip"
            elif "transaction {" in msg_lower:
                return "transaction"
            elif "sending slack notification" in msg_lower:
                return "slack_notification"
            elif "error" in msg_lower or "failed" in msg_lower:
                return "error"
            elif "overdraft" in msg_lower:
                return "overdraft"
            else:
                return "other"

        self.parsed_logs["message_category"] = self.parsed_logs["message"].apply(
            categorize_message
        )

        # Calculate category statistics
        category_stats = self.parsed_logs["message_category"].value_counts()
        self.analysis_results["category_stats"] = category_stats.to_dict()

        print(" Categorization complete")
        for cat, count in category_stats.items():
            percentage = (count / len(self.parsed_logs)) * 100
            print(f"   {cat}: {count:,} ({percentage:.1f}%)")

        return self.parsed_logs

    def extract_transactions(self):
        """
        This function helps in extracting the transaction related messages from
        the whole data provided.
        Also helps in parsing the transaction data provided in JSON format.

        """
        """Extract transaction data from logs"""
        print("\n-> Extracting transaction data...")

        transaction_logs = self.parsed_logs[
            self.parsed_logs["message_category"] == "transaction"
        ]
        transactions_data = []

        for _, log in transaction_logs.iterrows():
            msg = str(log["message"])

            # print(msg)

            # Extract transaction fields using regex
            tx_data = {
                "timestamp": log["timestamp"],
                "datetime": log["datetime"],
                "session_id": log["session_id"],
            }

            # Extract various fields
            patterns = {
                "id": r'"id":"([^"]+)"',
                "type": r'"type":"([^"]+)"',
                "source": r'"source":"([^"]+)"',
                "action": r'"action":"([^"]+)"',
                "amount": r'"amount":([\d.]+)',
                "vat": r'"vat":([\d.]+)',
                "userBalance": r'"userBalance":([\d.-]+)',
                "userId": r'"userId":"([^"]+)"',
            }

            for field, pattern in patterns.items():
                match = re.search(pattern, msg)
                if match:
                    if field in ["amount", "vat", "userBalance"]:
                        tx_data[field] = float(match.group(1))
                    else:
                        tx_data[field] = match.group(1)
                else:
                    tx_data[field] = None

            if tx_data.get("id") and tx_data.get("amount") is not None:
                transactions_data.append(tx_data)

        self.transactions = pd.DataFrame(transactions_data)

        if not self.transactions.empty:
            print(f" Extracted {len(self.transactions)} transactions")

            # Calculate transaction statistics
            credit_txs = self.transactions[self.transactions["type"] == "CREDIT"]
            debit_txs = self.transactions[self.transactions["type"] == "DEBIT"]

            self.analysis_results["transaction_stats"] = {
                "total_count": len(self.transactions),
                "credit_count": len(credit_txs),
                "debit_count": len(debit_txs),
                "total_credits": (
                    credit_txs["amount"].sum() if not credit_txs.empty else 0
                ),
                "total_debits": debit_txs["amount"].sum() if not debit_txs.empty else 0,
                "unique_users": self.transactions["userId"].nunique(),
                "unique_sessions": self.transactions["session_id"].nunique(),
            }

        else:
            print(" No transactions found in logs")
            self.analysis_results["transaction_stats"] = {
                "total_count": 0,
                "credit_count": 0,
                "debit_count": 0,
                "total_credits": 0,
                "total_debits": 0,
                "unique_users": 0,
                "unique_sessions": 0,
            }

        # print(self.analysis_results)

        return self.transactions

    def detect_overdrafts(self):
        """
        This function helps in identifying the overdraft transactions from the
        captured transaction data.
        Also finds potential overdraft cases.

        """
        """Detect potential overdraft situations"""
        print("\n-> Detecting overdrafts and negative balances...")

        overdrafts = []

        if not self.transactions.empty and "userBalance" in self.transactions.columns:
            # Find negative balances
            negative_balances = self.transactions[self.transactions["userBalance"] < 0]

            if not negative_balances.empty:
                print(
                    f" Found {len(negative_balances)} transactions with negative balance!"
                )
                overdrafts = negative_balances.copy()
            else:
                print(" No negative balances detected in available transaction data")

            # Find low balances (potential overdraft risk)
            low_balances = self.transactions[
                (self.transactions["userBalance"] >= 0)
                & (self.transactions["userBalance"] < 10)
            ]

            if not low_balances.empty:
                print(
                    f" Found {len(low_balances)} transactions with low balance (<$10)"
                )

            self.analysis_results["overdraft_stats"] = {
                "negative_balance_count": len(negative_balances),
                "low_balance_count": len(low_balances),
                "at_risk_users": (
                    low_balances["userId"].nunique() if not low_balances.empty else 0
                ),
            }
        else:
            print(" Cannot detect overdrafts - insufficient transaction data")
            self.analysis_results["overdraft_stats"] = {
                "negative_balance_count": 0,
                "low_balance_count": 0,
                "at_risk_users": 0,
            }
        # print(self.analysis_results)

        return pd.DataFrame(overdrafts)

    def analyze_user_patterns(self):
        """
        This function helps in generating a report per identified user
        transations.

        """
        print("\n-> Analyzing user patterns...")

        user_analysis = []

        if not self.transactions.empty:
            for user_id in self.transactions["userId"].unique():
                if pd.notna(user_id):
                    user_txs = self.transactions[
                        self.transactions["userId"] == user_id
                    ].sort_values("datetime")

                    if not user_txs.empty:
                        user_stats = {
                            "user_id": user_id,
                            "transaction_count": len(user_txs),
                            "credit_count": len(user_txs[user_txs["type"] == "CREDIT"]),
                            "debit_count": len(user_txs[user_txs["type"] == "DEBIT"]),
                            "total_credits": user_txs[user_txs["type"] == "CREDIT"][
                                "amount"
                            ].sum(),
                            "total_debits": user_txs[user_txs["type"] == "DEBIT"][
                                "amount"
                            ].sum(),
                            "min_balance": (
                                user_txs["userBalance"].min()
                                if "userBalance" in user_txs.columns
                                else None
                            ),
                            "max_balance": (
                                user_txs["userBalance"].max()
                                if "userBalance" in user_txs.columns
                                else None
                            ),
                            "current_balance": (
                                user_txs.iloc[-1]["userBalance"]
                                if "userBalance" in user_txs.columns
                                else None
                            ),
                            "first_transaction": user_txs.iloc[0]["timestamp"],
                            "last_transaction": user_txs.iloc[-1]["timestamp"],
                        }

                        # Check for overdraft
                        if (
                            user_stats["min_balance"] is not None
                            and user_stats["min_balance"] < 0
                        ):
                            user_stats["overdraft_flag"] = True
                        else:
                            user_stats["overdraft_flag"] = False

                        user_analysis.append(user_stats)

        user_analysis_df = pd.DataFrame(user_analysis)

        if not user_analysis_df.empty:
            print(f"Analyzed {len(user_analysis_df)} users")
            overdraft_users = user_analysis_df[
                user_analysis_df["overdraft_flag"] == True
            ]
            if not overdraft_users.empty:
                print(f" {len(overdraft_users)} users experienced overdraft")
        else:
            print(" No user data available for analysis")

        return user_analysis_df

    def export_results(self):
        """
        This function helps in eporting all analysis results to CSV files

        """
        print(f"\n-> Exporting results to CSV files...")
        output_prefix: str = "calo_analysis"

        # 1 Export transaction details
        if self.transactions is not None and not self.transactions.empty:
            tx_file = f"{output_prefix}_transactions.csv"
            # self.transactions.to_csv(tx_file, index=False)
            self.transactions.to_csv(os.path.join(OUTPUT_DIR, tx_file), index=False)
            print(f"Transaction details: {tx_file}")

        # 2. Export user analysis
        user_analysis = self.analyze_user_patterns()
        if not user_analysis.empty:
            user_file = f"{output_prefix}_user_analysis.csv"
            # user_analysis.to_csv(user_file, index=False)
            user_analysis.to_csv(os.path.join(OUTPUT_DIR, user_file), index=False)
            print(f"User analysis: {user_file}")

        # 3. Export category statistics
        category_stats = pd.DataFrame(
            list(self.analysis_results["category_stats"].items()),
            columns=["Category", "Count"],
        )
        category_stats["Percentage"] = (
            category_stats["Count"] / category_stats["Count"].sum() * 100
        )
        category_file = f"{output_prefix}_category_stats.csv"
        # category_stats.to_csv(category_file, index=False)
        category_stats.to_csv(os.path.join(OUTPUT_DIR, category_file), index=False)
        print(f"Category statistics: {category_file}")

        # 4. Export overdrafts
        overdrafts = self.detect_overdrafts()
        if not overdrafts.empty:
            overdraft_file = f"{output_prefix}_overdrafts.csv"
            # overdrafts.to_csv(overdraft_file, index=False)
            overdrafts.to_csv(os.path.join(OUTPUT_DIR, overdraft_file), index=False)
            print(f" Overdrafts: {overdraft_file}")

        print("\n All results exported successfully!")

        return {
            "transactions": (
                f"{output_prefix}_transactions.csv"
                if self.transactions is not None
                else None
            ),
            "user_analysis": f"{output_prefix}_user_analysis.csv",
            "category_stats": category_file,
            "overdrafts": (
                f"{output_prefix}_overdrafts.csv" if not overdrafts.empty else None
            ),
        }

    def run_complete_analysis(self):
        print("=" * 60)
        print("          CALO LOG ANALYSIS PIPELINE")
        print("=" * 60)
        # Step 1
        self.load_logs()

        # Step 2 - parsing the raw data
        self.parse_all_logs()

        # Step 3 -  Categorize logs
        self.categorize_logs()

        # Step 4 - Extract transactions
        self.extract_transactions()

        # Step 5 -  Detect overdrafts
        self.detect_overdrafts()

        # Step 6 -  Analyze user pattern
        self.analyze_user_patterns()

        self.export_results()


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Analyze Calo log files.")
    parser.add_argument(
        "log_file",
        nargs="?",
        default="log.csv",
        help="Path to the log file to be analyzed (default: log.csv)",
    )
    args = parser.parse_args()

    # Use the log_file argument from the command line
    analyzer = CaloLogAnalyzer(args.log_file)
    try:
        analyzer.run_complete_analysis()
        print("\nAnalysis completed successfully!")
        print("Check the 'output' directory for detailed CSV results.")
    except FileNotFoundError:
        print(f"\nERROR: The file '{args.log_file}' was not found.")
        print("Please make sure the file exists and the path is correct.")
    except Exception as e:
        print(f"\nERROR: An error occurred during analysis: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
