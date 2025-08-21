# target-gsheet

A [Singer](https://singer.io) target that writes data to Google
Sheets.

## How to use it

`target-gsheet` works together with any other [Singer Tap] to move
data from sources like [Braintree], [Freshdesk] and [Hubspot] to
Google Sheets. Google Sheets is a great, free way to review and
visualize data.

### Step 1: Activate the Google Sheets API

 (originally found in the [Google API
 docs](https://developers.google.com/sheets/api/quickstart/python))
 
 1. Use [this
 wizard](https://console.developers.google.com/start/api?id=sheets.googleapis.com)
 to create or select a project in the Google Developers Console and
 activate the Sheets API. Click Continue, then Go to credentials.

 1. On the **Add credentials to your project** page, click the
 **Cancel** button.

 1. At the top of the page, select the **OAuth consent screen**
 tab. Select an **Email address**, enter a **Product name** if not
 already set, and click the **Save** button.

 1. Select the **Credentials** tab, click the **Create credentials**
 button and select **OAuth client ID**.

 1. Select the application type **Other**, enter the name "Singer
 Sheets Target", and click the **Create** button.

 1. Click **OK** to dismiss the resulting dialog.

 1. Click the Download button to the right of the client ID.

 1. Move this file to your working directory and rename it
 *client_secret.json*.

### Step 2: Configure

Create a file called `config.json` in your working directory, following [config.sample.json](config.sample.json). 

#### Finding Your Spreadsheet ID

To find your spreadsheet ID, look at the URL of your Google Sheets spreadsheet. For example:

```
https://docs.google.com/spreadsheets/d/1qpyC0XzvTcKT6EISywvqESX3A0MwQoFDE8p-Bll4hps/edit#gid=0
```

The spreadsheet ID is the value between `/d/` and `/edit`: `1qpyC0XzvTcKT6EISywvqESX3A0MwQoFDE8p-Bll4hps`

#### Configuration Options

You can configure the target in two ways:

**Option 1: Using spreadsheet_id**
```json
{
    "spreadsheet_id": "1qpyC0XzvTcKT6EISywvqESX3A0MwQoFDE8p-Bll4hps",
    "batch_size": 1000
}
```

**Option 2: Using files array**
```json
{
    "files": [
        {
            "id": "1qpyC0XzvTcKT6EISywvqESX3A0MwQoFDE8p-Bll4hps",
            "name": "My Data Sheet"
        }
    ],
    "batch_size": 1000
}
```

#### Configuration Parameters

- **`spreadsheet_id`** (string, optional): The ID of the Google Sheets spreadsheet to write data to. Required if `files` is not provided.

- **`files`** (array, optional): An array of spreadsheet objects containing `id` and optional `name` fields. Required if `spreadsheet_id` is not provided. Use this format for compatibility with multi-file configurations.

- **`batch_size`** (integer, optional, default: 1000): The number of records to process in each batch operation. Larger batch sizes can improve performance for large datasets but may use more memory. Adjust based on your data size and system resources.

### Step 3: Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](python-mac) or
[Ubuntu](python-ubuntu).

`target-gsheet` can be run with any [Singer Tap], but we'll use
[`tap-fixerio`][Fixerio] - which pulls currency exchange rate data
from a public data set - as an example.

These commands will install `tap-fixerio` and `target-gsheet` with
pip and then run them together, piping the output of `tap-fixerio` to
`target-gsheet`:


```bash
› pip install target-gsheet tap-fixerio
› tap-fixerio | target-gsheet -c config.json
  INFO Replicating the latest exchange rate data from fixer.io
  INFO Tap exiting normally
```

If you're using a different Tap, substitute `tap-fixerio` in the final
command above to the command used to run your Tap.

`target-gsheet` will attempt to open a new window or tab in your
default browser. If this fails, copy the URL from the console and
manually open it in your browser.

If you are not already logged into your Google account, you will be
prompted to log in. If you are logged into multiple Google accounts,
you will be asked to select one account to use for the
authorization. Click the **Accept** button to allow `target-gsheet` to
access your Google Sheet.  You can close the tab after the signup flow
is complete.

The data will be written to a sheet named `exchange_rate` in your
Google Sheet.

---

Copyright &copy; 2017 Stitch

[Singer Tap]: https://singer.io
[Braintree]: https://github.com/singer-io/tap-braintree
[Freshdesk]: https://github.com/singer-io/tap-freshdesk
[Hubspot]: https://github.com/singer-io/tap-hubspot
[Fixerio]: https://github.com/singer-io/tap-fixerio
[python-mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[python-ubuntu]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
