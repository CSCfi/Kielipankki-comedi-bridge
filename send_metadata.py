import click


def metashare_cmdi_records(metashare_api_url):
    """
    Iterate over all records in META-SHARE in CMDI format.
    """
    print("TODO: get records")
    yield from ()


def upload_cmdi_to_comedi(cmdi_record, comedi_upload_url):
    """
    Upload the given XML record to COMEDI
    """
    print("TODO: upload record")


@click.command()
@click.option("--metashare-api-url", default="https://kielipankki.fi/md_api/")
@click.option("--comedi-upload-url", default="https://clarino.uib.no/comedi/upload")
def send_metadata(metashare_api_url, comedi_upload_url):
    """
    Send all metadata from META-SHARE to COMEDI.
    """
    for cmdi_record in metashare_cmdi_records(metashare_api_url):
        upload_cmdi_to_comedi(cmdi_record, comedi_upload_url)


if __name__ == "__main__":
    # pylint does not understand click wrappers
    # pylint: disable=no-value-for-parameter
    send_metadata()
