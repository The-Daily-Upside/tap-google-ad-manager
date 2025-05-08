# tap-google-ad-manager

`tap-google-ad-manager` is a Singer tap designed for extracting data from Google Ad Manager.

This tap is built using the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

---

## Installation

Install the tap using the following command:

```bash
pipx install git+https://github.com/The-Daily-Upside/tap-google-ad-manager.git
```

---

## Configuration

### Supported Configuration Options

The following configuration options are now supported:

* **`service_account_key_file`**: The path to your Google Service Account key JSON file.
* **`network_id`**: The network ID for your Google Ad Manager account.
* **`reports`**: A dictionary of report configurations, where each key is a report name and the value is the report definition.

> **Note**: Service Accounts are used instead of OAuth for authentication. Ensure you have downloaded the JSON key file for your Service Account from the Google Cloud Console.

### How Report Execution Works

Reports are defined in the `reports` section of the configuration. On each tap run:

1. The tap checks if the configured reports exist in Google Ad Manager.
2. If they do not exist, it programmatically creates them.
3. The tap then runs each report and waits for its completion.
4. Once completed, results are fetched using the `ReportResultsStream` and loaded into the target system.

This ensures all reporting is centrally defined and reliably refreshed on each tap execution.

### Example Configuration File

```json
{
  "service_account_key_file": "/path/to/your-service-account-key.json",
  "network_id": "your-network-id",
  "reports": {
    "line_items_last_year": {
      "displayName": "line_items_last_year",
      "reportDefinition": {
        "dimensions": [
          "DATE",
          "ORDER_ID",
          "ORDER_NAME",
          "LINE_ITEM_ID",
          "LINE_ITEM_NAME"
        ],
        "metrics": [
          "IMPRESSIONS",
          "CLICKS",
          "CTR"
        ],
        "dateRange": {
          "relative": "LAST_365_DAYS"
        },
        "reportType": "HISTORICAL"
      }
    },
    "creatives_last_year": {
      "displayName": "creatives_last_year",
      "reportDefinition": {
        "dimensions": [
          "DATE",
          "ORDER_ID",
          "ORDER_NAME",
          "LINE_ITEM_ID",
          "LINE_ITEM_NAME",
          "CREATIVE_ID",
          "CREATIVE_NAME",
          "CREATIVE_TYPE",
          "CREATIVE_TYPE_NAME",
          "RENDERED_CREATIVE_SIZE"
        ],
        "metrics": [
          "IMPRESSIONS",
          "CLICKS",
          "CTR"
        ],
        "dateRange": {
          "relative": "LAST_365_DAYS"
        },
        "reportType": "HISTORICAL"
      }
    }
  }
}
```

To view the full list of supported settings and capabilities, run:

```bash
tap-google-ad-manager --about
```

---

## Usage

### Running the Tap

You can execute the tap directly using the following commands:

```bash
tap-google-ad-manager --version
tap-google-ad-manager --help
tap-google-ad-manager --config config.json --discover > catalog.json
tap-google-ad-manager --config config.json --catalog catalog.json
```

#### Example Reports:

* **Line Items Last Year**: Includes impressions, clicks, and CTR over the past year.
* **Creatives Last Year**: Includes creative metadata and metrics over the past year.

#### Logs

The tap provides detailed logs for report creation, execution, and data fetching. Look for logs like:

* `📡 [ensure_reports_exist] GET ...`
* `🏃 [run_report] POST ...`
* `📥 Fetch rows body: ...`

---

### Key Files and Directories

* **`tap_google_ad_manager/`**: Contains the main implementation of the tap, including:

  * `client.py`: Handles API requests and authentication.
  * `streams.py`: Defines the data streams for Google Ad Manager resources, including reports.
  * `tap.py`: Entry point for the tap.
* **`meltano.yml`**: Configuration file for Meltano integration.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
