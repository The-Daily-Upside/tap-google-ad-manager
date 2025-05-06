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

- **`service_account_key_file`**: The path to your Google Service Account key JSON file.
- **`network_id`**: The network ID for your Google Ad Manager account.

> **Note**: Service Accounts are used instead of OAuth for authentication. Ensure you have downloaded the JSON key file for your Service Account from the Google Cloud Console.

### Example Configuration File

```json
{
  "service_account_key_file": "/path/to/your-service-account-key.json",
  "network_id": "your-network-id"
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

---

### Key Files and Directories

- **`tap_google_ad_manager/`**: Contains the main implementation of the tap, including:
  - `client.py`: Handles API requests and authentication.
  - `streams.py`: Defines the data streams for Google Ad Manager resources.
  - `tap.py`: Entry point for the tap.
- **`meltano.yml`**: Configuration file for Meltano integration.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
