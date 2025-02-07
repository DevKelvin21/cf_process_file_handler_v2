# Cloud Function Process File Handler v2

This repository contains the implementation of a Cloud Function designed to handle file processing tasks. The function is triggered by events in a cloud storage bucket and performs various operations on the files.

## Features

- **Event-driven**: Automatically triggered by cloud storage events.
- **Scalable**: Designed to handle large volumes of files efficiently.
- **Configurable**: Easily customizable to fit different processing needs.

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Google Cloud SDK

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/cf_process_file_handler_v2.git
    cd cf_process_file_handler_v2
    ```

2. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

### Deployment

1. Set up your Google Cloud project and enable the necessary APIs.
2. Deploy the Cloud Function:
    ```sh
        gcloud functions deploy process_file_v2 \
        --runtime python312 \
        --trigger-topic YOUR_PUBSUB_TOPIC \
        --entry-point process_file_v2 \
        --region YOUR_REGION \
        --set-env-vars BLACKLIST_API_KEY=YOUR_API_KEY,OUTPUT_BUCKET=your-output-bucket
    ```

## Usage

Once deployed, the Cloud Function will automatically trigger and process files uploaded to the specified cloud storage bucket.

## Configuration

You can customize the behavior of the Cloud Function by modifying the `config.json` file. This file contains various settings such as file types to process, processing parameters, and more.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or issues, please open an issue on GitHub or contact the repository owner.
