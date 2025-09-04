# Calo Subscriber Balance Log Analysis Pipeline

This project provides an automated pipeline for ingesting, parsing, and analyzing semi-structured log files from the Calo subscriber balance system. The primary goal is to transform raw text-based logs into structured data to identify key financial trends, transaction patterns, and subscriber overdraft events.

The pipeline processes directories of compressed log files (.gz), extracts meaningful data, categorizes log events, and generates a series of summary CSV files for review by accounting and engineering teams

# Technology Choices

The technologies were chosen for their efficiency, portability, and widespread adoption in the data engineering ecosystem.

1. Python
    Why: Python is the go to language for data analysis due to its simplicity, readability, and the vast ecosystem of data manipulation libraries. 
    Its scripting capabilities are perfect for creating a repeatable analysis pipeline.

2. Pandas Library
    Why: Pandas is the cornerstone of this analysis. It provides the DataFrame, a powerful and efficient data structure for handling tabular data. 

3. Docker
    Why: Docker solves the "it works on my machine" problem. By creating a container, we package the Python interpreter, the pandas library, and our script into a single, isolated unit. This guarantees that the analysis will run with zero configuration or installation errors on any system that has Docker installed, making it highly portable and reliable.


# Implementation Details

The analysis is encapsulated within the CaloLogAnalyzer class in analyzer.py. The pipeline executes in a series of sequential steps:

1. Step 1: Loading Logs (load_logs)

    The script takes a directory path as a command-line argument.

    It uses os.walk() to recursively scan the directory and all its subdirectories for files ending in .gz. This is a robust way to find all log files, regardless of the folder structure.

    Each .gz file is opened, and lines containing "INFO" are read into a list. This pre-filters out non-essential log lines like START or REPORT.

    Finally, all the collected log lines are loaded into a single pandas DataFrame with one column: raw_log.

2. Step 2: Parsing and Structuring (parse_all_logs)

    This step iterates through each raw log string.

    It uses a combination of .split('\t') and a regular expression (re.match) to reliably extract the four primary components: timestamp, session_id, message_type, and the main message.

    The extracted data is converted into a structured DataFrame, and a datetime column is created from the timestamp string for chronological sorting and analysis.

3. Step 3: Categorization (categorize_logs)

    To make sense of the data, each log message is classified into a specific category (e.g., processing_message, transaction, balance_sync_skip).

    This is done by applying a function that checks for keywords in the message string.

    This step produces valuable statistics on the frequency of different system events, helping to identify if any event type is occurring more or less than expected.

4. Step 4: Transaction Extraction (extract_transactions)

    This is the most critical data extraction step. It filters for logs categorized as transaction.

    It uses a dictionary of regular expressions to parse the JSON-like string within the transaction message, extracting key financial data like userId, amount, type (CREDIT/DEBIT), and userBalance.

    This structured financial data is stored in a new self.transactions DataFrame, which is the foundation for all subsequent financial analysis.

5. Step 5 & 6: Overdraft Detection & User Analysis

    Overdrafts (detect_overdrafts): The script performs a simple but powerful check on the transactions DataFrame:

    It filters for any transaction where userBalance < 0 to identify overdraft events.

    It also flags users with a low balance (0 <= userBalance < 10) as "at-risk."

    User Patterns (analyze_user_patterns): To provide a high-level summary for the accounting team, the script groups all transactions by userId. It uses the .groupby().agg() method to efficiently calculate key metrics for each user:

    Total transaction count

    Total credit/debit amounts

    Minimum and maximum balance

    An overdraft_flag to easily identify users who have had a negative balance.

6. Step 7: Exporting Results (export_results)

    The final step is to save the generated DataFrames into CSV files in the output directory, making the results accessible for review in Excel or other tools.

# How to Run with Docker

To run this analysis, you must have Docker Desktop installed and running on your machine.
1. Build the Docker Image

```text
docker build -t calo-analyzer .
```
2. Run the Analysis
```
docker run --rm -v "source_file_path :/app/data:ro" -v "${PWD}\output:/app/output" calo-analyzer /app/data

example - 
docker run --rm -v "C:\Users\Noufa\Documents\Codes\Coding challenges\calo\balance-sync-logs\balance-sync-logs:/app/data:ro" -v "${PWD}\output:/app/output" calo-analyzer /app/data

```

# Future Improvements

1. Configuration File: Instead of hardcoding values like log keywords, a config.yaml file could be used to define these, making the script more adaptable to changes in log formats.

2. Enhanced Anomaly Detection: Implement more advanced statistical methods to detect anomalies, such as identifying users with an unusually high transaction frequency or rapid balance fluctuations (e.g., using standard deviation).

3. Database Integration: Instead of exporting to CSVs, the results could be written directly to a SQL or NoSQL database. This would make the data available for querying and integration with BI tools like Tableau or Power BI for creating interactive dashboards.

4. Orchestration: For a production environment, the Docker container could be run on a schedule using a workflow orchestrator like Apache Airflow or Prefect, turning this one-off script into a fully automated, recurring data pipeline.