import requests

import click
import lxml
from sickle import Sickle, oaiexceptions


def metashare_cmdi_records(metashare_api_url):
    """
    Iterate over all records in META-SHARE in CMDI format.
    """
    sickle = Sickle(metashare_api_url)
    metadata_records = sickle.ListIdentifiers(
        **{
            "metadataPrefix": "info",
            "ignore_deleted": True,
        }
    )
    for record in metadata_records:
        for metadata_format in ["cmdi0554", "cmdi0571", "cmdi2312", "cmdi9836"]:
            try:
                response = sickle.GetRecord(
                    identifier=record.identifier, metadataPrefix=metadata_format
                )
            except oaiexceptions.CannotDisseminateFormat:
                continue
            print(record.identifier)
            yield response.xml
            break


def upload_cmdi_to_comedi(metashare_record, comedi_upload_url, session_id, published):
    """
    Upload the given XML record to COMEDI

    In case of errors, a message is printed to stdout and the problematic record is
    skipped.
    """
    try:
        cmdi_record = metashare_record.xpath(
            "oai:metadata/cmd:CMD",
            namespaces={
                "oai": "http://www.openarchives.org/OAI/2.0/",
                "cmd": "http://www.clarin.eu/cmd/",
            },
        )[0]
        xml_content = lxml.etree.tostring(
            cmdi_record, xml_declaration=True, encoding="utf-8"
        )
    except IndexError:
        metashare_identifier = metashare_record.xpath(
            "oai:header/oai:identifier/text()",
            namespaces={"oai": "http://www.openarchives.org/OAI/2.0/"},
        )[0]
        print(f"No CMDI record found for {metashare_identifier}")
        return

    try:
        urn_url = cmdi_record.xpath(
            "cmd:Components/cmd:resourceInfo/cmd:identificationInfo/cmd:identifier/text()",
            namespaces={"cmd": "http://www.clarin.eu/cmd/"},
        )[0]
    except IndexError:
        metashare_identifier = metashare_record.xpath(
            "oai:header/oai:identifier/text()",
            namespaces={"oai": "http://www.openarchives.org/OAI/2.0/"},
        )[0]
        print(f"No urn found for {metashare_identifier}")
        return

    try:
        urn = urn_url.split("urn:nbn:fi:")[1]
    except IndexError:
        print(f"Could not parse urn {urn_url}")
        return

    params = {
        "group": "FIN-CLARIN",
        "session-id": session_id,
    }
    if published:
        params["published"] = True

    response = requests.post(
        comedi_upload_url,
        params=params,
        files={"file": (f"{urn}.xml", xml_content, "text/xml")},
    )
    response.raise_for_status()

    if "error" in response.json():
        print(f"Upload failed: {response.json()['error']}")
    elif "success" not in response.json() or not response.json()["success"]:
        print("Something went wrong: {response.json()}")


@click.command()
@click.argument("comedi_session_id")
@click.option("--metashare-api-url", default="https://kielipankki.fi/md_api/que")
@click.option("--comedi-upload-url", default="https://clarino.uib.no/comedi/upload")
@click.option("--publish/--unpublish", default=False)
def send_metadata(comedi_session_id, metashare_api_url, comedi_upload_url, publish):
    """
    Send all metadata from META-SHARE to COMEDI.
    """
    records = 0
    for cmdi_record in metashare_cmdi_records(metashare_api_url):
        records += 1
        upload_cmdi_to_comedi(
            cmdi_record,
            comedi_upload_url,
            session_id=comedi_session_id,
            published=publish,
        )
    print(f"{records} processed")


if __name__ == "__main__":
    # pylint does not understand click wrappers
    # pylint: disable=no-value-for-parameter
    send_metadata()
